-module(interclock_db).

-behaviour(gen_server).

%% API
-export([start_link/5,
         identity/1, fork/1, join/2, retire/1,
         read/2, peek/2, write/3, sync/4, delete/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {id, uuid, dir, log, db_path, db_ref}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name, UUID, Id, Path, Type) ->
    gen_server:start_link(?MODULE, {Name, Id, UUID, Path, Type}, []).

identity(Name) ->
    gen_server:call({via, gproc, {n,l,Name}}, identity).

fork(Name) ->
    gen_server:call({via, gproc, {n,l,Name}}, fork, timer:seconds(30)).

join(Name, OtherId) ->
    gen_server:call({via, gproc, {n,l,Name}}, {join, OtherId}, timer:seconds(30)).

retire(Name) ->
    gen_server:call({via, gproc, {n,l,Name}}, retire, timer:seconds(30)).

read(Name, Key) when is_binary(Key) ->
    gen_server:call({via, gproc, {n,l,Name}}, {read, Key}).

peek(Name, Key) when is_binary(Key) ->
    gen_server:call({via, gproc, {n,l,Name}}, {peek, Key}).

write(Name, Key, Val) when is_binary(Key) ->
    gen_server:call({via, gproc, {n,l,Name}}, {write, Key, Val}).

sync(Name, Key, EventClock, Val=[_|_]) when is_binary(Key) ->
    gen_server:call({via, gproc, {n,l,Name}}, {sync, Key, EventClock, Val});
sync(Name, Key, EventClock, Val={deleted,_,_}) when is_binary(Key) ->
    gen_server:call({via, gproc, {n,l,Name}}, {sync, Key, EventClock, Val}).

delete(Name, Key) when is_binary(Key) ->
    gen_server:call({via, gproc, {n,l,Name}}, {delete, Key}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({Name, Id, UUID, Path, Type}) ->
    case interclock_manager:reserve(Name, UUID, Path, Type) of
        {error, exists} ->
            {stop, exists};
        ok ->
            process_flag(trap_exit, true),
            Log = filename:join(Path, "log"),
            Db = filename:join(Path, "db"),
            ok = filelib:ensure_dir(Log),
            ok = filelib:ensure_dir(Db),
            log({booted, calendar:local_time(), UUID, Id, []}, Log),
            Ref = bitcask:open(Db, [read_write]),
            TrustedId = ensure_identity(Ref, UUID, Id, Log),
            gc(Ref), % GC on boot every time
            gc_timer(),
            {ok, #state{id=TrustedId, uuid=UUID, dir=Path, log=Log,
                        db_path=Db, db_ref=Ref}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(identity, _From, State=#state{id=Id, uuid=UUID}) ->
    {reply, {UUID, Id}, State};

handle_call(fork, _From, State=#state{id=Id, uuid=UUID, log=Log, db_ref=Db}) ->
    {NewClock,PeerClock} = itc:fork(itc:rebuild(Id, undefined)),
    {NewId,_} = itc:explode(NewClock),
    {PeerId,_} = itc:explode(PeerClock),
    log({forked, calendar:local_time(), UUID, Id, [{NewId,PeerId}]}, Log),
    ok = db_write_sync(Db, <<"id">>, NewId),
    {reply, {UUID, PeerId}, State#state{id=NewId}};

handle_call({join, OtherId}, _From, State=#state{id=Id, uuid=UUID, log=Log, db_ref=Db}) ->
    case maybe_join(Id, OtherId) of
        {error, _} ->
            {reply, bad_id, State};
        {ok, NewId} ->
            log({joined, calendar:local_time(), UUID, Id, [OtherId]}, Log),
            ok = db_write_sync(Db, <<"id">>, NewId),
            {reply, {UUID, NewId}, State#state{id=NewId}}
    end;

handle_call(retire, _From, State=#state{id=Id, uuid=UUID, log=Log, db_path=Db, db_ref=Ref}) ->
    %% We shut down the node. Kill the logs, and the DB, then return the
    %% identity.
    _ = file:delete(Log),
    %% Bitcask has no function to delete the DB, so we must go for it manually
    bitcask:close(Ref),
    Files = filelib:wildcard(Db++"/*"),
    [file:delete(File) || File <- Files],
    file:del_dir(Db),
    {stop, {shutdown, retired}, {UUID, Id}, State};

handle_call({read, Key}, _From, State=#state{db_ref=Db}) ->
    Reply = case db_read(Db, Key) of
        {ok, {_Event, [Val]}} -> % 1 value, we're good
            {ok, Val};
        {ok, {_Event, Vals=[_|_]}} -> % >1 values, conflict!
            {conflict, Vals};
        {ok, {_Event, {deleted, _Stamp, Vals=[_|_]}}} ->
            {conflict, deleted, Vals};
        {ok, {_Event, {deleted, _Stamp, []}}} ->
            {error, undefined};
        {error, undefined} -> % not found, woe is me
            {error, undefined}
    end,
    {reply, Reply, State};

handle_call({peek, Key}, _From, State=#state{db_ref=Db}) ->
    Reply = case db_read(Db, Key) of
        {ok, {Event, Vals}} -> {ok, Event, Vals};
        {error, undefined} -> {error, undefined}
    end,
    {reply, Reply, State};

handle_call({write, Key, Val}, _From, State=#state{db_ref=Db, id=Id}) ->
    case db_read(Db, Key) of
        {ok, {Event, _}} -> % value already exists
            db_write_sync(Db, Key, {incr(Id, Event), [Val]});
        {error, undefined} -> % first insert ever
            db_write_sync(Db, Key, {incr(Id, undefined), [Val]})
    end,
    {reply, ok, State};

handle_call({sync, Key, PeerEvent, PeerVal}, _From, State=#state{db_ref=Db, id=Id}) ->
    PeerClock = peer_clock(Id, PeerEvent),
    case db_read(Db, Key) of
        {ok, {LocalEvent, LocalVal}} ->
            LocalClock = local_clock(Id, LocalEvent),
            Response = sync(Db, Key, PeerClock, PeerVal, LocalClock, LocalVal),
            {reply, Response, State};
        {error, undefined} ->
            db_write(Db, Key, {PeerEvent, PeerVal}),
            {reply, peer, State}
    end;

handle_call({delete, Key}, _From, State=#state{db_ref=Db, id=Id}) ->
    case db_read(Db, Key) of
        {ok, {Event, _}} -> % value already exists
            db_write_sync(Db, Key, {incr(Id, Event), {deleted, epoch(), []}});
        {error, undefined} -> % first insert ever
            db_write_sync(Db, Key, {incr(Id, undefined), {deleted, epoch(), []}})
    end,
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(garbage_collect, State=#state{db_ref=Db}) ->
    %% For manual GC calls
    gc(Db),
    {noreply, State};
handle_info({timeout, _Ref, gc}, State=#state{db_ref=Db}) ->
    %% automated GC calls
    gc(Db),
    gc_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate({shutdown, retired}, #state{}) ->
    %% Don't attempt to close stuff, we deleted it already.
    ok;
terminate(Reason, #state{uuid=UUID, id=Id, log=Log, db_ref=Db}) ->
    bitcask:close(Db),
    log({terminated, calendar:local_time(), UUID, Id, [{reason, Reason}]}, Log),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
log(Term, FileName) ->
    {ok, IoDevice} = file:open(FileName, [append]),
    ok = io:format(IoDevice, "~p.~n", [Term]),
    ok = file:datasync(IoDevice),
    file:close(IoDevice).

ensure_identity(Ref, UUID, Id, Log) ->
    case {bitcask:get(Ref, <<"id">>), bitcask:get(Ref, <<"uuid">>)} of
        {not_found, not_found} ->
            %% first opening
            ok = bitcask:put(Ref, <<"id">>, term_to_binary(Id)),
            ok = bitcask:put(Ref, <<"uuid">>, term_to_binary(UUID)),
            bitcask:sync(Ref),
            Id;
        {not_found, {ok, _Bin}} -> 
            %% Maybe we crashed during first opening, but we should fail
            %% and require manual intervention for now
            error(id_not_found);
        {{ok,_Bin}, not_found} ->
            %% Maybe we crashed during first opening, but we should fail
            %% and require manual intervention for now
            error(uuid_not_found);
        {{ok,BinId}, {ok,BinUUID}} ->
            %% There was table existing, likely healthy.
            case {binary_to_term(BinId), binary_to_term(BinUUID)} of
                {Id, UUID} ->
                    %% All terms fit, good.
                    Id;
                {DiskId, UUID} ->
                    %% This is our table by the UUID, but the ID passed in is
                    %% outdated. This can be due to a restart with supervisor
                    %% arguments saved in, or following a failed fork or join.
                    id_from_logs(Ref, UUID, DiskId, Log);
                {_, DiskUUID} ->
                    %% This is not our table!
                    error({uuid_conflict, UUID, DiskUUID})
             end
     end.

db_read(Ref, Key) ->
    case bitcask:get(Ref, Key) of
        {ok, Bin} -> {ok, binary_to_term(Bin)};
        not_found -> {error, undefined}
    end.

db_write(Ref, Key, Val) ->
    bitcask:put(Ref, Key, term_to_binary(Val)).

db_write_sync(Ref, Key, Val) ->
    bitcask:put(Ref, Key, term_to_binary(Val)),
    bitcask:sync(Ref).

id_from_logs(Ref, UUID, DiskId, Log) ->
    {ok, Data} = file:consult(Log),
    %% We write we booted before entering here, so let's drop this and go for
    %% second-to-last and find forks or merges.
    Pos = length(Data)-1,
    %% Consistency check on the DiskId -- it should be within this log
    %% file somewhere, otherwise we may have a configuration error
    %% on our hands.
    %% Now let's recover.
    case lists:nth(Pos, Data) of
        {forked, _TS, UUID, DiskId, [{Id, _}]} ->
            %% That's our last fork, likely a failed one.
            %% Pick the new id from there.
            %% We need to check the lineage, as the disk id
            %% should be present in history
            in_lineage(DiskId, Data),
            recover(Ref, UUID, Id, failed_fork, Log),
            Id;
        {forked, _TS, UUID, _SomeId, [{DiskId, _}]} ->
            %% That's our last fork, which succeeded.
            %% The disk Id may or may not be in the lineage
            %% depending on merge history.
            recover(Ref, UUID, DiskId, fork, Log),
            DiskId;
        {joined, _TS, UUID, DiskId, [OtherId]} ->
            %% That's our last join, likely a failed one. Merge back
            %% the IDs to keep running.
            {ok, Id} = maybe_join(DiskId, OtherId),
            recover(Ref, UUID, Id, failed_join, Log),
            Id;
        {joined, _TS, UUID, OldId, [OtherId]} ->
            %% That's our last join, likely a failed one. Merge back
            %% the IDs to keep running.
            case maybe_join(OldId, OtherId) of
                {ok, DiskId} ->
                    recover(Ref, UUID, DiskId, join, Log),
                    DiskId;
                {error, _} ->
                    error(unstable_join_state)
            end;
        {recovered, _TS, UUID, DiskId, _Details} ->
            %% We recovered once before. Let's trust this entry.
            %% It should therefore be in the past lineage
            recover(Ref, UUID, DiskId, recovery, Log),
            DiskId;
        {terminated, _TS, UUID, DiskId, _Details} ->
            recover(Ref, UUID, DiskId, termination, Log),
            DiskId;
        {_Term, _Ts, OtherUUID, _DiskId, _Details} when OtherUUID =/= UUID ->
            error({uuid_conflict, OtherUUID, UUID});
        _ ->
            in_lineage(DiskId, Data),
            %% This is abnormal?! If it were a regular boot (i.e.
            %% first one, without forks or joins or recoveries),
            %% our ID should have fit.
            error({failed_recover, UUID, DiskId})
    end.

recover(Ref, UUID, Id, Reason, Log) ->
    ok = bitcask:put(Ref, <<"id">>, term_to_binary(Id)),
    bitcask:sync(Ref),
    log({recovered, calendar:local_time(), UUID, Id, [Reason]}, Log).

in_lineage(DiskId, Logs) ->
    AllIds = [element(4, Term) || Term <- Logs],
    case lists:member(DiskId, AllIds) of
        false -> error({bad_lineage, DiskId});
        true -> ok
    end.

maybe_join(IdA, IdB) ->
    try
        Clock = itc:join(itc:rebuild(IdA, undefined),
                         itc:rebuild(IdB, undefined)),
        {NewId, _} = itc:explode(Clock),
        {ok, NewId}
    catch
        Class:Reason -> % ooh a risky catch-all
            {error, {Class,Reason}}
    end.


local_clock(Id, Event) ->
    itc:rebuild(Id, Event).
peer_clock(Id, Event) ->
    itc:peek(itc:rebuild(Id, Event)).

incr(Id, undefined) ->
    {_, Event} = itc:explode(itc:rebuild(Id, undefined)),
    Event;
incr(Id, Event) ->
    {_, NewEvent} = itc:explode(itc:event(itc:rebuild(Id, Event))),
    NewEvent.

join_events(PeerClock, LocalClock) ->
    {_,NewEvent} = itc:explode(itc:join(PeerClock, LocalClock)),
    NewEvent.

sync(Db, Key, PeerClock, PeerData, LocalClock, LocalData) ->
    NewEvent = join_events(PeerClock, LocalClock),
    case itc:leq(PeerClock, LocalClock) of
        true ->
            db_write(Db, Key, {NewEvent, LocalData}),
            local;
        false ->
            case itc:leq(LocalClock, PeerClock) of
                true ->
                    db_write(Db, Key, {NewEvent, PeerData}),
                    peer;
                false ->
                    conflict(Db, Key, NewEvent, LocalData, PeerData),
                    conflict
            end
    end.

%% Handle difference between deletions and regular writes.
conflict(Db, Key, NewEvent, LocalData, PeerData) ->
    case {LocalData, PeerData} of
        {{deleted, SLocal, Local}, {deleted, SPeer, Peer}} -> % 2 deletions
            Stamp = max(SLocal, SPeer),
            db_write(Db, Key, {NewEvent, {deleted, Stamp, Local++Peer}});
        {{deleted, Stamp, Local}, _} -> % 1 deletion
            db_write(Db, Key, {NewEvent, {deleted, Stamp, Local++PeerData}});
        {_, {deleted, Stamp, Peer}} -> % 1 deletion
            db_write(Db, Key, {NewEvent, {deleted, Stamp, LocalData++Peer}});
        {_, _} -> % writes only
            db_write(Db, Key, {NewEvent, LocalData++PeerData})
    end.

epoch() ->
    {Mega, Sec, _Micro} = os:timestamp(),
    (Mega * 1000000) + Sec.

gc(Db) ->
    {ok, Delay} = application:get_env(interclock, gc_delay),
    Threshold = epoch() - Delay,
    OldFun = fun(Key, ValBin, Acc) ->
        case binary_to_term(ValBin) of
            {_, {deleted, Epoch, []}} when Epoch =< Threshold -> [Key|Acc];
            _ -> Acc
        end
    end,
    ToDelete = bitcask:fold(Db, OldFun, []),
    [bitcask:delete(Db, Key) || Key <- ToDelete],
    ok.

gc_timer() ->
    {ok, IntervalSecs} = application:get_env(interclock, gc_interval),
    erlang:start_timer(timer:seconds(IntervalSecs),
                       self(),
                       gc).
