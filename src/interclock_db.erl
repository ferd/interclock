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
            ok = ensure_identity(Ref, UUID, Id),
            {ok, #state{id=Id, uuid=UUID, dir=Path, log=Log,
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
    ok = write(Db, <<"id">>, NewId),
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

ensure_identity(Ref, UUID, Id) ->
    case {bitcask:get(Ref, <<"id">>), bitcask:get(Ref, <<"uuid">>)} of
        {not_found, not_found} -> % first opening
            ok = bitcask:put(Ref, <<"id">>, term_to_binary(Id)),
            ok = bitcask:put(Ref, <<"uuid">>, term_to_binary(UUID));
        {not_found, _Bin} ->
            error(id_not_found);
        {_Bin, not_found} ->
            error(uuid_not_found);
        {BinId, BinUUID} ->
            case {binary_to_term(BinId), binary_to_term(BinUUID)} of
                {Id, UUID} -> ok;
                {Id, BadUUID} -> error({uuid_conflict, UUID, BadUUID});
                {BadId, UUID} -> error({id_conflict, Id, BadId});
                {BadId, BadUUID} -> error({identity_conflict, {Id,BadId},
                                                              {UUID,BadUUID}})
             end
     end.

write(Ref, Key, Val) ->
    bitcask:put(Ref, Key, term_to_binary(Val)).
