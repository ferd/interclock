-module(interclock_manager).

-export([reserve/4]).

%% Registers and checks for availability of resources through gproc.
%% ITC IDs are purposedly left external, to be processed by each individual
%% DB managers.
-spec reserve(Name, UUID, Path, Type) -> ok | {error, exists} when
    Name :: interclock:name(),
    UUID :: interclock:uuid(),
    Path :: file:filename(),
    Type :: interclock:type().
reserve(Name, UUID, Path, Type) ->
    AnyName = {n,l,'_'},
    Pid = '$0',
    BasePatterns = [{{{n,l,Name}, Pid, '_'}, [], [Pid]}, % same name
                    {{AnyName, Pid, {'_', Path, '_'}}, [], [Pid]}], % same path
    Patterns = BasePatterns
               ++
               if Type =:= root -> % two master
                    [{{AnyName, Pid, {UUID, '$2', 'root'}}, [], [Pid]}];
                  Type =/= root ->
                    []
               end,
    case find(Patterns) of
        [] -> % It's free!
            try
                gproc:reg({n,l,Name}, {UUID, Path, Type}),
                check_for_conflicts(Patterns)
            catch
                error:badarg ->
                    {error, exists}
            end;
        _L -> % It's taken
            {error, exists}
    end.

%% Gproc kind of sucks there and doesn't support complex matchspecs for
%% its select/1 operation, so we do many at once.
find(Patterns) ->
    lists:usort(lists:append([gproc:select([Pattern]) || Pattern <- Patterns])).

%% We however have a potential race condition that can be prompted if many
%% people try to register similar databases at once -- they could all pass
%% the initial check, then be registered with similar properties but different
%% names.
%%
%% We do a second round of checks, and if more than one entry exists with
%% similar properties, we keep the one with the smallest Pid and return an
%% error for the rest.
%%
%% This may give temporary instability, but should be limited in time, and
%% within the scope of interclock_db's start_link function, which in normal
%% use will yield a synchronous start. Outsiders should see now ill effect,
%% except in the rare case where a db process crashes while the user tries
%% to register one again over the same Name or Path, in which case they would
%% theoretically pick up where the other left.
check_for_conflicts(Patterns) ->
    case find(Patterns) of
        [_] -> % one entry, that's us
            ok;
        [_|_] = List -> % more than one entry, conflict!
            case {self(), lists:min(List)} of
                {X,X} -> ok; % we win
                _ -> {error, exists} % we lose
            end
    end.
