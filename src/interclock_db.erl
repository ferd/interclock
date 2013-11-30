-module(interclock_db).

-behaviour(gen_server).

%% API
-export([start_link/5,
         id/1, fork/1]).

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

id(Name) ->
    gen_server:call({via, gproc, {n,l,Name}}, id).

fork(Name) ->
    gen_server:call({via, gproc, {n,l,Name}}, fork, timer:seconds(30)).

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
handle_call(id, _From, State=#state{id=Id, uuid=UUID}) ->
    {reply, {UUID, Id}, State};
handle_call(fork, _From, State=#state{id=Id, uuid=UUID, log=Log, db_ref=Db}) ->
    {NewClock,PeerClock} = itc:fork(itc:rebuild(Id, undefined)),
    {NewId,_} = itc:explode(NewClock),
    {PeerId,_} = itc:explode(PeerClock),
    log({forked, calendar:local_time(), UUID, Id, [{NewId,PeerId}]}, Log),
    ok = write_sync(Db, <<"id">>, NewId),
    {reply, {UUID, PeerId}, State#state{id=NewId}};
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
                    error({uuid_conflict, UUID, DiskUUID});
                {_, DiskUUID} -> error({uuid_conflict, {UUID,DiskUUID}})
             end
     end.

write(Ref, Key, Val) ->
    bitcask:put(Ref, Key, term_to_binary(Val)).

write_sync(Ref, Key, Val) ->
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
        {recovered, _TS, UUID, DiskId, _Details} ->
            %% We recovered once before. Let's trust this entry.
            %% It should therefore be in the past lineage
            in_lineage(DiskId, Data),
            recover(Ref, UUID, DiskId, recovery, Log),
            DiskId;
        _ ->
            %% This is abnormal?! If it were a regular boot (i.e.
            %% first one, without forks or joins or recoveries),
            %% our ID should have fit.
            error({failed_recover, UUID, DiskId, Log})
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
