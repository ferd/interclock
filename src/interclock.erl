-module(interclock).

-export([boot/2, identity/1, fork/1, join/2, retire/1]).
-export([read/2, peek/2, write/3, sync/4]).

-type name() :: term().
-type type() :: 'root' | 'normal'.
-type id() :: itc:id().
-type uuid() :: iodata().

-export_type([name/0, type/0, id/0, uuid/0]).

%%%===================================================================
%%% API
%%%===================================================================
boot(Name, Opts) ->
    Path = proplists:get_value(dir, Opts),
    case {proplists:get_value(type, Opts, normal),
                       proplists:get_value(uuid, Opts),
                       proplists:get_value(id, Opts)} of
        {root, UUID, undefined} when UUID =/= undefined->
            {ClockId, _ClockEvent} = itc:explode(itc:seed()),
            start_process(Name, UUID, ClockId, Path, root);
        {root, UUID, ClockId} when UUID =/= undefined,
                                   ClockId =/= undefined ->
            start_process(Name, UUID, ClockId, Path, root);
        {normal, ExistingUUID, undefined} when ExistingUUID =/= undefined ->
            %% Maybe rebooting! Trust the recovery mechanisms.
            start_process(Name, ExistingUUID, undefined, Path, normal);
        {normal, ExistingUUID, ClockId} when ClockId =/= undefined,
                                             ExistingUUID =/= undefined ->
            start_process(Name, ExistingUUID, ClockId, Path, normal);
        {_, _, _} ->
            {error, missing_identity}
    end.

identity(Name) ->
    interclock_db:identity(Name).

fork(Name) ->
    interclock_db:fork(Name).

join(Name, OtherId) ->
    interclock_db:join(Name, OtherId).

retire(Name) ->
    interclock_db:retire(Name).

read(Name, Key) ->
    interclock_db:read(Name, Key).

peek(Name, Key) ->
    interclock_db:peek(Name, Key).

write(Name, Key, Val) ->
    interclock_db:write(Name, Key, Val).

sync(Name, Key, EventClock, Val) ->
    interclock_db:sync(Name, Key, EventClock, Val).

%%%===================================================================
%%% private
%%%===================================================================
start_process(Name, UUID, Id, Path, Type) ->
    case supervisor:start_child(interclock_sup, [Name, UUID, Id, Path, Type]) of
        {error, Reason} -> {error, Reason};
        {ok, _Pid} -> ok
    end.
