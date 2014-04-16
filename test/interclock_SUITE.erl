-module(interclock_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() -> [{group, boot},
          {group, fork},
          {group, join},
          {group, read_write},
          {group, delete}].

groups() ->
    [{boot, [],
      [boot_root, boot_slave, {group, recovery}]},
     {recovery, [],
      [fork_recovery, crashed_fork_recovery, recovery_good, shutdown_recovery,
       recovery_recovery, bad_lineage_recovery, bad_uuid_recovery,
       bad_log_uuid_recovery, crashed_join_recovery, join_recovery]},
     {fork, [],
      [fork_standalone]},
     {join, [],
      [join_standalone, join_bad_id, join_shutdown]},
     {read_write, [],
      [read_write, read_write_sync, read_write_simulate_sync, list_keys]},
     {delete, [],
      [delete, delete_sync]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

group(_GroupName) ->
    [].

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

%% Fork Inits
init_per_testcase(fork_standalone, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "fork_standalone"),
    Name = fork_standalone,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    [{name,Name}, {uuid, UUID}, {db, Db}, {log, LogFile},
     {started, Started} | Config];
%% Join Inits
init_per_testcase(join_standalone, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "join_standalone"),
    Name = join_standalone,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    {UUID, NewFork} = interclock:fork(Name),
    [{name,Name}, {uuid, UUID}, {db, Db}, {log, LogFile}, {fork, NewFork},
     {started, Started} | Config];
init_per_testcase(join_bad_id, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "join_bad_id"),
    Name = join_bad_id,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    [{name,Name}, {uuid, UUID}, {db, Db}, {log, LogFile},
     {started, Started} | Config];
init_per_testcase(join_shutdown, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "join_shutdown"),
    Name = join_shutdown,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    [{name,Name}, {uuid, UUID}, {db, Db}, {log, LogFile},
     {started, Started} | Config];
init_per_testcase(read_write, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "read_write"),
    Name = read_write,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    [{name,Name}, {uuid, UUID}, {db, Db}, {log, LogFile},
     {started, Started} | Config];
init_per_testcase(read_write_sync, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "read_write_sync"),
    PathAlt = filename:join(?config(priv_dir, Config), "read_write_sync_fork"),
    Name = read_write_sync,
    ForkName = read_write_sync_fork,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    {UUID, Fork} = interclock:fork(Name),
    ok = interclock:boot(ForkName, [{type, normal}, {dir, PathAlt},
                                     {uuid, UUID}, {id, Fork}]),
    [{root_name,Name}, {fork_name,ForkName}, {uuid, UUID}, {db, Db},
     {log, LogFile}, {started, Started} | Config];
init_per_testcase(read_write_simulate_sync, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "read_write_simulate_sync"),
    PathAlt = filename:join(?config(priv_dir, Config), "read_write_simulate_sync_fork"),
    Name = read_write_simulate_sync,
    ForkName = read_write_simulate_sync_fork,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    {UUID, Fork} = interclock:fork(Name),
    ok = interclock:boot(ForkName, [{type, normal}, {dir, PathAlt},
                                     {uuid, UUID}, {id, Fork}]),
    [{root_name,Name}, {fork_name,ForkName}, {uuid, UUID}, {db, Db},
     {log, LogFile}, {started, Started} | Config];
init_per_testcase(list_keys, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "list_keys"),
    Name = list_keys,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    [{root_name,Name}, {uuid, UUID}, {db, Db},
     {log, LogFile}, {started, Started} | Config];
init_per_testcase(delete, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "delete"),
    Name = delete,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    [{name,Name}, {uuid, UUID}, {db, Db}, {log, LogFile},
     {started, Started} | Config];
init_per_testcase(delete_sync, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    Path = filename:join(?config(priv_dir, Config), "delete_sync"),
    PathAlt = filename:join(?config(priv_dir, Config), "delete_sync_fork"),
    Name = delete_sync,
    ForkName = delete_sync_fork,
    UUID = uuid:get_v4(),
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    ok = interclock:boot(Name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    {UUID, Fork} = interclock:fork(Name),
    ok = interclock:boot(ForkName, [{type, normal}, {dir, PathAlt},
                                     {uuid, UUID}, {id, Fork}]),
    [{root_name,Name}, {fork_name,ForkName}, {uuid, UUID}, {db, Db},
     {log, LogFile}, {started, Started} | Config];
%% Rest
init_per_testcase(_TestCase, Config) ->
    {ok, Started} = application:ensure_all_started(interclock),
    [{started, Started} | Config].

end_per_testcase(_TestCase, Config) ->
    [application:stop(App) || App <- lists:reverse(?config(started, Config))],
    ok.

%%%===================================================================
%%% Booting up Test cases
%%%===================================================================

boot_root(Config) ->
    Path = filename:join(?config(priv_dir, Config), "boot_root"),
    PathAlt = filename:join(?config(priv_dir, Config), "boot_root_other"),
    UUID = uuid:get_v4(),
    {error, missing_identity} = interclock:boot(root_name, [{uuid, UUID},
                                                            {dir, Path}]),
    ok = interclock:boot(root_name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    {SeedId,_SeedEv} = itc:explode(itc:seed()),
    {UUID, SeedId} = interclock:identity(root_name),
    %% Same name / type / path
    {error, exists} = interclock:boot(root_name, [{type, root}, {dir, Path},
                                                  {uuid, UUID}]),
    %% No id sent with a non-root, means we fail.
    {error, missing_identity} = interclock:boot(root_name, [{type, normal},
                                                            {dir, Path}]),
    {error, missing_identity} = interclock:boot(root_name, [{type, normal},
                                                            {id, 1},
                                                            {dir, Path}]),
    %% Undefined path
    {error, undefined_dir} = interclock:boot(root_name, [{type, normal},
                                                         {uuid, UUID}]),
    %% Same uuid / path
    {error, exists} = interclock:boot(root_name, [{type, normal},
                                                  {uuid, UUID},
                                                  {dir, Path}]),
    %% Same name / path
    {error, exists} = interclock:boot(root_name, [{type, normal}, {dir, Path},
                                                  {uuid, uuid:get_v4()}, {id, 1}]),
    %% Same name
    {error, exists} = interclock:boot(root_name, [{type, normal}, {dir, PathAlt},
                                                  {uuid, uuid:get_v4()}, {id, 1}]),
    %% Same path
    {error, exists} = interclock:boot(root_other, [{type, normal}, {dir, Path},
                                                   {uuid, uuid:get_v4()}, {id, 1}]),
    %% Same type is okay, given they don't get the same UUID nor path
    ok = interclock:boot(root_other, [{type, root}, {dir, PathAlt},
                                      {uuid, <<"fakeuuid">>}]),
    {UUIDOther, SeedId} = interclock:identity(root_other),
    ?assertNotEqual(UUIDOther, UUID).

boot_slave(Config) ->
    Path = filename:join(?config(priv_dir, Config), "boot_slave"),
    PathAlt = filename:join(?config(priv_dir, Config), "boot_slave_other"),
    UUID = uuid:get_v4(),
    ok = interclock:boot(root_boot, [{type, root}, {dir, Path},
                                     {uuid, UUID}]),
    {SeedId,_SeedEv} = itc:explode(itc:seed()),
    {UUID, SeedId} = interclock:identity(root_boot),
    {UUID, Fork} = interclock:fork(root_boot), % returns the new fork to send away
    {UUID, Forked} = interclock:identity(root_boot), % has its own forked id
    ?assertEqual(itc:rebuild(SeedId,undefined),
                 itc:join(itc:rebuild(Fork, undefined),
                          itc:rebuild(Forked, undefined))),
    %% Be *very* careful to use the right id to boot the replica!
    ok = interclock:boot(root_boot_slave, [{type, normal}, {dir, PathAlt},
                                            {uuid, UUID}, {id, Fork}]),
    {ok,RootLog} = file:consult(filename:join(Path, "log")),
    {ok,AltLog} = file:consult(filename:join(PathAlt, "log")),
    [{booted, _, UUID, SeedId, []},
     {forked, _, UUID, SeedId, [{Forked,Fork}]}
     |_] = RootLog,
    ct:pal("~p", [{UUID, Fork}]),
    [{booted, _, UUID, Fork, []} | _] = AltLog.

%%%===================================================================
%%% Recovery Test cases
%%%===================================================================

%% Test for a fork operation with an outdated id in the start command.
%% This may happen whenever a process is restarted with an outdated ID
%% or whenever someone's config isn't up to date on actors' booting, which
%% may as well be the majority of the cases.
fork_recovery(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "fork_recovery"),
    %% Put in a DB that failed in a stable state, following a fork
    Db = filename:join(Path, "db"),
    disk_state(Db, good_id, UUID),
    %% Simulate a log file that resulted in that fork being there
    LogFile = filename:join(Path, "log"),
    fake_log([{booted, 1, []},
              {forked, 1, [{bad_id, other_id}]},
              {forked, bad_id, [{good_id, another_id}]}], % the fork worked
             UUID,
             LogFile),
    %% Boot the db, with an outdated id -- using 'bad_id' should also
    %% work. What we want here is make sure that an outdated id on
    %% a restart lands us with the correct on-disk id
    ok = interclock:boot(fork_recovery, [{type, normal}, {dir, Path},
                                         {id, 1}, {uuid, UUID}]),
    {UUID, good_id} = interclock:identity(fork_recovery), % it recovered
    %% The internal log should show this
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{bad_id, other_id}]},
     {forked, _, UUID, bad_id, [{good_id, another_id}]},
     {booted, _, UUID, 1, []},
     {recovered, _, UUID, good_id, [fork]} | _] = NewLog.

%% Test with a fork operation that has crashed halfway through (never made it
%% to another node) -- this can be detected through the DB's logs having the
%% fork operation, but the actual DB not having the matching id. To avoid most
%% potential conflicts (and even bugs or people breaking the API in some ways),
%% we always restore from the newest operation possible.
crashed_fork_recovery(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "crashed_fork"),
    %% Put in a DB that failed in a crashed state -- a bad id is present in it
    Db = filename:join(Path, "db"),
    disk_state(Db, bad_id, UUID),
    %% Simulate a log file that resulted in that bad id prior to a fork
    LogFile = filename:join(Path, "log"),
    fake_log([{booted, 1, []},
              {forked, 1, [{bad_id, other_id}]},
              {forked, bad_id, [{good_id, another_id}]}], % the fork failed
             UUID,
             LogFile),
    %% Boot the db, and make sure we use the id we should have forked.
    ok = interclock:boot(crashed_fork, [{type, root}, {dir, Path},
                                        {id, good_id}, {uuid, UUID}]),
    {UUID, good_id} = interclock:identity(crashed_fork), % it recovered
    %% The internal log should show this
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{bad_id, other_id}]},
     {forked, _, UUID, bad_id, [{good_id, another_id}]},
     {booted, _, UUID, good_id, []},
     {recovered, _, UUID, good_id, [failed_fork]} | _] = NewLog.

%% The node can be booted with the right id, in which case it works
%% without needing to add any more logs than its own booting.
recovery_good(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "recovery_good"),
    %% Put in a DB that failed in a good stable state
    Db = filename:join(Path, "db"),
    disk_state(Db, good_id, UUID),
    %% Simulate a log file that resulted in that disk id following many
    %% crashes and recoveries.
    LogFile = filename:join(Path, "log"),
    fake_log([{booted, 1, []},
              {forked, 1, [{good_id, other_id}]},
              {booted, 1, []},
              {recovered, good_id, [failed_fork]}], % the recovery from fork worked
             UUID,
             LogFile),
    %% Boot the db, with an good id
    ok = interclock:boot(recovery_good, [{type, normal}, {dir, Path},
                                         {id, good_id}, {uuid, UUID}]),
    {UUID, good_id} = interclock:identity(recovery_good), % it recovered
    %% The internal log should show no recovery needed
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{good_id, other_id}]},
     {booted, _, UUID, 1, []},
     {recovered, _, UUID, good_id, [failed_fork]},
     {booted, _, UUID, good_id, []} | _] = NewLog.

%% The node may have an outdated ID when being started, but the actual good id
%% has been logged as part of an orderly shutdown. The node should be able to
%% recover from the that record.
shutdown_recovery(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "shutdown_recovery"),
    %% Put in a DB that failed in a good stable state
    Db = filename:join(Path, "db"),
    disk_state(Db, good_id, UUID),
    %% Simulate a log file that resulted in that disk id following many
    %% crashes and recoveries.
    LogFile = filename:join(Path, "log"),
    fake_log([{booted, 1, []},
              {forked, 1, [{good_id, other_id}]},
              {terminated, good_id, [some_reason]}],
             UUID,
             LogFile),
    %% Boot the db, with an outdated id -- using 'bad_id' should also
    %% work. What we want here is make sure that an outdated id on
    %% a restart lands us with the correct on-disk id
    ok = interclock:boot(shutdown_recovery, [{type, normal}, {dir, Path},
                                             {id, undefined}, {uuid, UUID}]),
    {UUID, good_id} = interclock:identity(shutdown_recovery), % it recovered
    %% The internal log should show this
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{good_id, other_id}]},
     {terminated, _, UUID, good_id, [some_reason]},
     {booted, _, UUID, undefined, []}, % let the recovery mechanism do its thing
     {recovered, _, UUID, good_id, [termination]} | _] = NewLog.

%% The node may have an outdated ID when being started, but the actual good id
%% has been recovered before. The node should be able to recover from the
%% previous recovery record.
recovery_recovery(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "recovery_recovery"),
    %% Put in a DB that failed in a good stable state
    Db = filename:join(Path, "db"),
    disk_state(Db, good_id, UUID),
    %% Simulate a log file that resulted in that disk id following many
    %% crashes and recoveries.
    LogFile = filename:join(Path, "log"),
    fake_log([{booted, 1, []},
              {forked, 1, [{good_id, other_id}]},
              {booted, 1, []},
              {recovered, good_id, [failed_fork]}], % the fork worked
             UUID,
             LogFile),
    %% Boot the db, with an outdated id -- using 'bad_id' should also
    %% work. What we want here is make sure that an outdated id on
    %% a restart lands us with the correct on-disk id
    ok = interclock:boot(recovery_recovery, [{type, normal}, {dir, Path},
                                         {id, undefined}, {uuid, UUID}]),
    {UUID, good_id} = interclock:identity(recovery_recovery), % it recovered
    %% The internal log should show this
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{good_id, other_id}]},
     {booted, _, UUID, 1, []},
     {recovered, _, UUID, good_id, [failed_fork]},
     {booted, _, UUID, undefined, []}, % let the recovery mechanism do its thing
     {recovered, _, UUID, good_id, [recovery]} | _] = NewLog.

%% A node shouldn't be able to recover if the ID it has in the DB
%% isn't part of its own history according to the logs and could be the
%% result of someone manually squashing files.
bad_lineage_recovery(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "bad_lineage"),
    %% Put in a DB that failed in a good stable state
    Db = filename:join(Path, "db"),
    disk_state(Db, unknown_id, UUID),
    %% Simulate a log file that resulted in that disk id following many
    %% crashes and recoveries for a given set of ids
    LogFile = filename:join(Path, "log"),
    fake_log([{booted, 1, []},
              {forked, 1, [{good_id, other_id}]},
              {booted, 1, []},
              {recovered, good_id, [failed_fork]}], % the fork worked
             UUID,
             LogFile),
    %% Boot the db, with an id that was never part of its history
    {error, {{bad_lineage,unknown_id}, _Stacktrace}} =
        interclock:boot(bad_lineage, [{type, normal}, {dir, Path},
                                      {id, 1}, {uuid, UUID}]).

%% A node shouldn't be able to recover if the UUID it's been set up with
%% Doesn't correspond to the on-disk data it has.
bad_uuid_recovery(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "bad_uuid"),
    %% Put in a DB that failed in a good stable state
    Db = filename:join(Path, "db"),
    disk_state(Db, good_id, UUID),
    %% Simulate a log file that resulted in that disk id following many
    %% crashes and recoveries for a given set of ids
    LogFile = filename:join(Path, "log"),
    fake_log([{booted, 1, []},
              {forked, 1, [{good_id, other_id}]},
              {booted, 1, []},
              {recovered, good_id, [failed_fork]}], % the fork worked
             UUID,
             LogFile),
    %% Boot the db, with an id that was never part of its history
    {error, {{uuid_conflict,_,_}, _Stacktrace}} =
        interclock:boot(bad_uuid, [{type, normal}, {dir, Path},
                                      {id, 1}, {uuid, <<"new uuid">>}]).

%% A node shouldn't be able to recover if the UUID it's been set up with
%% Doesn't correspond to the in-log data it has.
bad_log_uuid_recovery(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "bad_log_uuid"),
    %% Put in a DB that failed in a good stable state
    Db = filename:join(Path, "db"),
    disk_state(Db, good_id, UUID),
    %% Simulate a log file that resulted in that disk id following many
    %% crashes and recoveries for a given set of ids
    LogFile = filename:join(Path, "log"),
    fake_log([{booted, 1, []},
              {forked, 1, [{good_id, other_id}]},
              {booted, 1, []},
              {recovered, good_id, [failed_fork]}], % the fork worked
             <<"not the good uuid">>,
             LogFile),
    %% Boot the db, with an id that was never part of its history
    {error, {{uuid_conflict,_,_}, _Stacktrace}} =
        interclock:boot(bad_log_uuid, [{type, normal}, {dir, Path},
                                      {id, 1}, {uuid, UUID}]).


%% For Id joining, the problem is particularly tricky because we have
%% two perspectives -- the receiver, and the node being retired.
%% The retired node doesn't matter in this case because all of its data
%% should go away. The receiver may crash similarly to a fork.
%% This test checks for a join as the last operation when the id provided
%% doesn't match the latest one or isn't provided at all, and that the
%% database ID seems to match the last join operation (but isn't up to date
%% on disk yet)
crashed_join_recovery(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "crashed_join_recovery"),
    %% Put in a DB that failed in a stable state, following a join
    Db = filename:join(Path, "db"),
    disk_state(Db, {0,1}, UUID),
    %% Simulate a log file that resulted in a join in place.
    %% The node should:
    %% 1. join the two ids in memory without storing it
    %% 2. log the join operation that has been proven to work
    %% 3. commit to disk.
    %% It is important that the join operation works before storing it.
    LogFile = filename:join(Path, "log"),
    fake_log([{booted, 1, []},
              {forked, 1, [{{0,1}, {1,0}}]},
              {joined, {0,1}, [{1,0}]}], % the join should result back in '1'
             UUID,
             LogFile),
    %% Boot the db, with an outdated or unknown id. What we want here is
    %% to make sure that an outdated id on a restart lands us with the correct
    %% on-disk id
    ok = interclock:boot(crashed_join_recovery, [{type, normal}, {dir, Path},
                                                 {id, undefined}, {uuid, UUID}]),
    {UUID, 1} = interclock:identity(crashed_join_recovery), % it recovered
    %% The internal log should show this
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{{0,1}, {1,0}}]},
     {joined, _, UUID, {0,1}, [{1,0}]},
     {booted, _, UUID, undefined, []},
     {recovered, _, UUID, 1, [failed_join]} | _] = NewLog.

%% This test checks for a join as the last operation when the id provided
%% doesn't match the latest one or isn't provided at all, but the id in the
%% DB is up to date regarding what the latest join operation would yield.
join_recovery(Config) ->
    UUID = <<"fake uuid">>,
    Path = filename:join(?config(priv_dir, Config), "join_recovery"),
    %% Put in a DB that failed in a stable state, following a join
    Db = filename:join(Path, "db"),
    LogFile = filename:join(Path, "log"),
    %% Here the Id on disk matches the id that the logs logically lead to,
    %% but isn't literally in there.
    disk_state(Db, 1, UUID),
    fake_log([{booted, 1, []},
              {forked, 1, [{{0,1}, {1,0}}]},
              {joined, {0,1}, [{1,0}]}], % the join should result back in '1'
             UUID,
             LogFile),
    %% Boot the db, with an outdated or unknown id. What we want here is
    %% to make sure that an outdated id on a restart lands us with the correct
    %% on-disk id
    ok = interclock:boot(join_recovery, [{type, normal}, {dir, Path},
                                         {id, undefined}, {uuid, UUID}]),
    {UUID, 1} = interclock:identity(join_recovery), % it recovered
    %% The internal log should show this
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{{0,1}, {1,0}}]},
     {joined, _, UUID, {0,1}, [{1,0}]},
     {booted, _, UUID, undefined, []},
     {recovered, _, UUID, 1, [join]} | _] = NewLog.

%%%===================================================================
%%% Fork Test cases
%%%===================================================================
fork_standalone(Config) ->
    Name = ?config(name, Config),
    {UUID, Id} = interclock:identity(Name),
    {UUID, NewFork} = interclock:fork(Name),
    {UUID, NewId} = interclock:identity(Name),
    %% The IDs are all different
    false = Id == NewFork orelse Id == NewId orelse NewFork == NewId,
    %% The new IDs are joinable to their old state, thus valid
    {Id, _} = itc:explode(itc:join(itc:rebuild(NewFork, undefined),
                                   itc:rebuild(NewId, undefined))),
    %% The logs contain all necessary IDs and operations, only once.
    {ok, Logs} = file:consult(?config(log, Config)),
    ForkMatch = fun({forked, _, LogUUID, LogId, [{ForkLeft,ForkRight}]}) ->
                    LogUUID =:= UUID andalso
                    LogId =:= Id andalso
                    ForkLeft =:= NewId andalso
                    ForkRight =:= NewFork
                ;  (_) -> false
                end,
    [1] = [1 || Log <- Logs, ForkMatch(Log)],
    %% The DB has been updated to the new ID
    {ok, NewId} = disk_read(?config(db, Config), <<"$id">>),
    {ok, UUID} = disk_read(?config(db, Config), <<"$uuid">>).

%%%===================================================================
%%% Join Test cases
%%%===================================================================
join_standalone(Config) ->
    Name = ?config(name, Config),
    ForkId = ?config(fork, Config),
    {UUID, Id} = interclock:identity(Name),
    %% The IDs are different
    true = Id =/= ForkId,
    %% The current IDs are joinable
    {NewId, _} = itc:explode(itc:join(itc:rebuild(Id, undefined),
                                      itc:rebuild(ForkId, undefined))),
    {UUID, NewId} = interclock:join(Name, ForkId),
    %% The logs contain all necessary IDs and operations, only once.
    %% Note that join entries do not happen to note the joined id.
    {ok, Logs} = file:consult(?config(log, Config)),
    JoinMatch = fun({joined, _, LogUUID, LogId, [JoinId]}) ->
                    LogUUID =:= UUID andalso
                    LogId =:= Id andalso
                    ForkId =:= JoinId
                ;  (_) -> false
                end,
    [1] = [1 || Log <- Logs, JoinMatch(Log)],
    %% The DB has been updated to the new ID
    {ok, NewId} = disk_read(?config(db, Config), <<"$id">>),
    {ok, UUID} = disk_read(?config(db, Config), <<"$uuid">>).

join_bad_id(Config) ->
    Name = ?config(name, Config),
    {UUID, Id} = interclock:identity(Name),
    %% join with itself or another bad term fails
    bad_id = interclock:join(Name, Id),
    bad_id = interclock:join(Name, hey_there),
    %% The logs contain no trace of these operations
    {ok, Logs} = file:consult(?config(log, Config)),
    JoinMatch = fun({joined, _, LogUUID, LogId, [_JoinId]}) ->
                    LogUUID =:= UUID andalso
                    LogId =:= Id
                ;  (_) -> false
                end,
    [] = [1 || Log <- Logs, JoinMatch(Log)],
    %% The DB has kept the old id
    {ok, Id} = disk_read(?config(db, Config), <<"$id">>),
    {ok, UUID} = disk_read(?config(db, Config), <<"$uuid">>).

join_shutdown(Config) ->
    %% We can retire a node, which will give us its id back and delete its
    %% files on disk.
    Name = ?config(name, Config),
    {ok, _} = file:consult(?config(log, Config)),
    {ok, Id} = disk_read(?config(db, Config), <<"$id">>),
    {_UUID, Id} = interclock:retire(Name),
    {error, enoent} = file:consult(?config(log, Config)),
    %% this prints a lot of errors, but that's fine
    {error, nofile} = disk_read(?config(db, Config), <<"$id">>),
    ?assertExit({noproc, _Stack}, interclock:identity(Name)).

%% All causational systems have a problem whenever the client isn't the
%% server itself, but may represent multiple users writing the data.
%% In such cases, it makes sense for the client to hold its own copy of the
%% data or event counter in order to detect conflicts between reads and
%% writes if they are to happen. The alternative is to go through DVVs
%% (http://arxiv.org/pdf/1011.5808v1.pdf), which can track conflicts server-
%% side only. The different solutions are:
%%
%% - Have the clients part of the clock
%% - Have stateless client enforce read-your-writes semantics
%% - pick up a maximum on the server to figure out a most recent entry to win
%% - DVV approach for tracking conflicts on a local node.
%%
%% Because Interclock is more intended for rare or sparse updates and intended
%% originally to be used server-side as much as possible, we consider the
%% server to be the main client and will ignore most issues until they become
%% really apparent.
read_write(Config) ->
    Name = ?config(name, Config),
    Id = ?config(id, Config),
    {error, undefined} = interclock:read(Name, <<"my_key">>),
    {error, undefined} = interclock:peek(Name, <<"my_key">>),
    ok = interclock:write(Name, <<"my_key">>, some_value),
    {ok, some_value} = interclock:read(Name, <<"my_key">>),
    {ok, Event1, [some_value]} = interclock:peek(Name, <<"my_key">>),
    ok = interclock:write(Name, <<"my_key">>, other_value),
    {ok, Event2, [other_value]} = interclock:peek(Name, <<"my_key">>),
    true = itc:leq(itc:rebuild(Id, Event1), itc:rebuild(Id, Event2)),
    false = itc:leq(itc:rebuild(Id, Event2), itc:rebuild(Id, Event1)).

read_write_sync(Config) ->
    Root = ?config(root_name, Config),
    Fork = ?config(fork_name, Config),
    %% Case 1: basic replication copies data as is given one copy leads
    %%         over another one.
    interclock:write(Fork, <<"key">>, val),
    {ok, Event1, Vals1} = interclock:peek(Fork, <<"key">>),
    peer = interclock:sync(Root, <<"key">>, Event1, Vals1),
    {ok, Event1, Vals1} = interclock:peek(Root, <<"key">>),
    %% Case 2: This can go both ways.
    interclock:write(Root, <<"key">>, val2),
    {ok, Event2, Vals2} = interclock:peek(Root, <<"key">>),
    ?assertNotEqual(Event1, Event2),
    peer = interclock:sync(Fork, <<"key">>, Event2, Vals2),
    {ok, Event2, Vals2} = interclock:peek(Fork, <<"key">>),
    %% Case 3: Concurrent writes conflict and are paired together, and
    %%         per-record. Conflicts are not in any particular order.
    interclock:write(Root, <<"key">>, new_val),
    interclock:write(Fork, <<"key">>, other_val),
    interclock:write(Fork, <<"heh">>, 1923.12),
    {ok, EventA, ValsA} = interclock:peek(Root, <<"key">>),
    {ok, EventB, ValsB} = interclock:peek(Fork, <<"key">>),
    {ok, EventC, ValsC} = interclock:peek(Fork, <<"heh">>),
    peer = interclock:sync(Root, <<"heh">>, EventC, ValsC),
    conflict = interclock:sync(Root, <<"key">>, EventB, ValsB),
    conflict = interclock:sync(Fork, <<"key">>, EventA, ValsA),
    %% we get conflict lists that are complete
    {conflict, L1} = interclock:read(Root, <<"key">>),
    {conflict, L2} = interclock:read(Fork, <<"key">>),
    ?assertEqual(lists:sort(L1), lists:sort(L2)),
    ?assertEqual(2, length(L1)),
    ?assert(lists:member(new_val, L1)),
    ?assert(lists:member(new_val, L2)),
    ?assert(lists:member(other_val, L1)),
    ?assert(lists:member(other_val, L2)),
    {ok, EventD, [_,_]} = interclock:peek(Root, <<"key">>),
    {ok, EventD, [_,_]} = interclock:peek(Fork, <<"key">>),
    %% other key is fine
    {ok, 1923.12} = interclock:read(Root, <<"heh">>),
    %% Case 4: A write can crush both and will later sync correctly
    %%         to the other node.
    interclock:write(Root, <<"key">>, crushed),
    {ok, EventLast, Crushed} = interclock:peek(Root, <<"key">>),
    peer = interclock:sync(Fork, <<"key">>, EventLast, Crushed),
    {ok, crushed} = interclock:read(Fork, <<"key">>).

read_write_simulate_sync(Config) ->
    Root = ?config(root_name, Config),
    Fork = ?config(fork_name, Config),
    %% Case 1: basic replication copies data as is given one copy leads
    %%         over another one.
    interclock:write(Fork, <<"key">>, val),
    {ok, Event1, Vals1} = interclock:peek(Fork, <<"key">>),
    %% No effect
    peer = interclock:simulate_sync(Root, <<"key">>, Event1, Vals1),
    {error, undefined} = interclock:peek(Root, <<"key">>),
    %% Sync for real
    peer = interclock:sync(Root, <<"key">>, Event1, Vals1),
    {ok, Event1, Vals1} = interclock:peek(Root, <<"key">>),
    %% Case 2: This can go both ways.
    interclock:write(Root, <<"key">>, val2),
    {ok, Event2, Vals2} = interclock:peek(Root, <<"key">>),
    ?assertNotEqual(Event1, Event2),
    peer = interclock:simulate_sync(Fork, <<"key">>, Event2, Vals2),
    {ok, Event1, Vals1} = interclock:peek(Fork, <<"key">>),
    peer = interclock:sync(Fork, <<"key">>, Event2, Vals2),
    {ok, Event2, Vals2} = interclock:peek(Fork, <<"key">>),
    %% Case 3: Concurrent writes conflict and are paired together, and
    %%         per-record. Conflicts are not in any particular order.
    interclock:write(Root, <<"key">>, new_val),
    interclock:write(Fork, <<"key">>, other_val),
    interclock:write(Fork, <<"heh">>, 1923.12),
    {ok, EventA, ValsA} = interclock:peek(Root, <<"key">>),
    {ok, EventB, ValsB} = interclock:peek(Fork, <<"key">>),
    {ok, EventC, ValsC} = interclock:peek(Fork, <<"heh">>),
    peer = interclock:simulate_sync(Root, <<"heh">>, EventC, ValsC),
    conflict = interclock:simulate_sync(Root, <<"key">>, EventB, ValsB),
    conflict = interclock:simulate_sync(Fork, <<"key">>, EventA, ValsA),
    %% Nothing conflicts for real due to simulations
    {ok, EventA, ValsA} = interclock:peek(Root, <<"key">>),
    {ok, EventB, ValsB} = interclock:peek(Fork, <<"key">>),
    {ok, EventC, ValsC} = interclock:peek(Fork, <<"heh">>),
    %% Sync for real
    peer = interclock:sync(Root, <<"heh">>, EventC, ValsC),
    conflict = interclock:sync(Root, <<"key">>, EventB, ValsB),
    conflict = interclock:sync(Fork, <<"key">>, EventA, ValsA),
    %% we get conflict lists that are complete
    {conflict, L1} = interclock:read(Root, <<"key">>),
    {conflict, L2} = interclock:read(Fork, <<"key">>),
    ?assertEqual(lists:sort(L1), lists:sort(L2)),
    ?assertEqual(2, length(L1)),
    ?assert(lists:member(new_val, L1)),
    ?assert(lists:member(new_val, L2)),
    ?assert(lists:member(other_val, L1)),
    ?assert(lists:member(other_val, L2)),
    {ok, EventD, [_,_]} = interclock:peek(Root, <<"key">>),
    {ok, EventD, [_,_]} = interclock:peek(Fork, <<"key">>),
    %% other key is fine
    {ok, 1923.12} = interclock:read(Root, <<"heh">>),
    %% Case 4: A write can crush both and will later sync correctly
    %%         to the other node.
    interclock:write(Root, <<"key">>, crushed),
    {ok, EventLast, Crushed} = interclock:peek(Root, <<"key">>),
    peer = interclock:simulate_sync(Fork, <<"key">>, EventLast, Crushed),
    {ok, crushed} = interclock:read(Root, <<"key">>),
    {conflict, L2} = interclock:read(Fork, <<"key">>),
    peer = interclock:sync(Fork, <<"key">>, EventLast, Crushed),
    {ok, crushed} = interclock:read(Fork, <<"key">>).

list_keys(Config) ->
    Root = ?config(root_name, Config),
    %% Case 1: basic replication copies data as is given one copy leads
    %%         over another one.
    interclock:write(Root, <<"key1">>, val),
    interclock:write(Root, <<"key2">>, val),
    interclock:write(Root, <<"key3">>, val),
    interclock:write(Root, <<"key4">>, val),
    [<<"key1">>,
     <<"key2">>,
     <<"key3">>,
     <<"key4">>] = lists:sort(interclock:keys(Root)).

%% Deletion is a tricky case that may end up requiring tombstones!
delete(Config) ->
    Name = ?config(name, Config),
    Id = ?config(id, Config),
    {error, undefined} = interclock:read(Name, <<"my_key">>),
    ok = interclock:write(Name, <<"my_key">>, some_value),
    {ok, some_value} = interclock:read(Name, <<"my_key">>),
    ok = interclock:delete(Name, <<"my_key">>),
    {error, undefined} = interclock:read(Name, <<"my_key">>),
    {ok, Event1, {deleted, _TS, []}} = interclock:peek(Name, <<"my_key">>),
    ok = interclock:write(Name, <<"my_key">>, other_value),
    {ok, Event2, [other_value]} = interclock:peek(Name, <<"my_key">>),
    true = itc:leq(itc:rebuild(Id, Event1), itc:rebuild(Id, Event2)),
    false = itc:leq(itc:rebuild(Id, Event2), itc:rebuild(Id, Event1)).

delete_sync(Config) ->
    Root = ?config(root_name, Config),
    Fork = ?config(fork_name, Config),
    %% Case 1: basic replication copies data as is given one copy leads
    %%         over another one.
    interclock:write(Fork, <<"key">>, val),
    interclock:delete(Fork, <<"key">>),
    {ok, Event1, Vals1} = interclock:peek(Fork, <<"key">>),
    peer = interclock:sync(Root, <<"key">>, Event1, Vals1),
    {ok, Event1, Vals1} = interclock:peek(Root, <<"key">>),
    %% Case 2: This can go both ways.
    interclock:write(Root, <<"key">>, val2),
    interclock:delete(Root, <<"key">>),
    {ok, Event2, Vals2} = interclock:peek(Root, <<"key">>),
    ?assertNotEqual(Event1, Event2),
    peer = interclock:sync(Fork, <<"key">>, Event2, Vals2),
    {ok, Event2, Vals2} = interclock:peek(Fork, <<"key">>),
    %% Case 3: Concurrent writes and deletes conflict and are paired together,
    %%         and per-record. Conflicts are not in any particular order.
    interclock:write(Root, <<"key">>, new_val),
    interclock:delete(Root, <<"key">>),
    interclock:write(Fork, <<"key">>, other_val),
    {ok, EventA, ValsA} = interclock:peek(Root, <<"key">>),
    {ok, EventB, ValsB} = interclock:peek(Fork, <<"key">>),
    conflict = interclock:sync(Root, <<"key">>, EventB, ValsB),
    conflict = interclock:sync(Fork, <<"key">>, EventA, ValsA),
    %% we get conflict lists that are complete and contain deletion vs.
    %% values info
    {conflict, deleted, ValsB} = interclock:read(Root, <<"key">>),
    {conflict, deleted, ValsB} = interclock:read(Fork, <<"key">>),
    {ok, EventD, {deleted, _, ValsB}} = interclock:peek(Root, <<"key">>),
    {ok, EventD, {deleted, _, ValsB}} = interclock:peek(Fork, <<"key">>),
    %% Case 4: A write can crush both and will later sync correctly
    %%         to the other node.
    interclock:write(Root, <<"key">>, crushed),
    {ok, EventLast, Crushed} = interclock:peek(Root, <<"key">>),
    peer = interclock:sync(Fork, <<"key">>, EventLast, Crushed),
    {ok, crushed} = interclock:read(Fork, <<"key">>),
    %% Case 5: A delete to a conflict including deletion asserts the
    %%         deletion. The higher event clock means that when re-
    %%         syncing, we confirm the deletion and crush the conflicted
    %%         bits.
    interclock:delete(Root, <<"key">>),
    interclock:write(Fork, <<"key">>, new),
    {ok, EventE, ValsE} = interclock:peek(Root, <<"key">>),
    {ok, EventF, ValsF} = interclock:peek(Fork, <<"key">>),
    conflict = interclock:sync(Fork, <<"key">>, EventE, ValsE),
    conflict = interclock:sync(Root, <<"key">>, EventF, ValsF),
    {ok, _, {deleted, _, [_|_]}} = interclock:peek(Root, <<"key">>),
    {ok, _, {deleted, _, [_|_]}} = interclock:peek(Fork, <<"key">>),
    interclock:delete(Root, <<"key">>),
    interclock:write(Fork, <<"key">>, new),
    {ok, EventG, {deleted, _, []}} = interclock:peek(Root, <<"key">>),
    {ok, _, [new]} = interclock:peek(Fork, <<"key">>),
    %% Case 6: Multiple writes are preserved when syncing.
    conflict = interclock:sync(Fork, <<"key">>, EventG, {deleted, 10, [old]}),
    {conflict, deleted, List} = interclock:read(Fork, <<"key">>),
    [new, old] = lists:sort(List).

%% Todo: deletion tombstone garbage collection.

%%%===================================================================
%%% Helpers
%%%===================================================================
disk_state(Db, Id, UUID) ->
    Ref = bitcask:open(Db, [read_write]),
    ok = bitcask:put(Ref, <<"$id">>, term_to_binary(Id)),
    ok = bitcask:put(Ref, <<"$uuid">>, term_to_binary(UUID)),
    ok = bitcask:close(Ref).

disk_read(Db, Key) ->
    Ref = bitcask:open(Db),
    case bitcask:get(Ref, Key) of
        {ok, Bin} -> {ok, binary_to_term(Bin)};
        Other -> Other
    end.

fake_log(List, UUID, LogFile) ->
    {ok, IoDevice} = file:open(LogFile, [append]),
    fake_log1(List, UUID, IoDevice).

fake_log1([], _, IoDevice) -> file:close(IoDevice);
fake_log1([{Type, Id, Terms}|Rest], UUID, IoDevice) ->
    io:format(IoDevice, "~p.~n", [{Type, calendar:local_time(), UUID, Id, Terms}]),
    fake_log1(Rest, UUID, IoDevice).
