-module(interclock_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [boot_root, boot_slave,
     fork_recovery, crashed_fork_recovery, recovery_good,
     recovery_recovery, bad_lineage_recovery, bad_uuid_recovery,
     bad_log_uuid_recovery].

groups() ->
    [].

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
    ok = interclock:boot(root_name, [{type, root}, {dir, Path}, {uuid, UUID}]),
    {SeedId,_SeedEv} = itc:explode(itc:seed()),
    {UUID, SeedId} = interclock:id(root_name),
    %% Same name / type / path
    {error, exists} = interclock:boot(root_name, [{type, root}, {dir, Path},
                                                  {uuid, UUID}]),
    %% No id sent with a non-root, means we fail.
    {error, missing_identity} = interclock:boot(root_name, [{type, normal},
                                                            {dir, Path}]),
    {error, missing_identity} = interclock:boot(root_name, [{type, normal},
                                                            {id, 1},
                                                            {dir, Path}]),
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
    {UUIDOther, SeedId} = interclock:id(root_other),
    ?assertNotEqual(UUIDOther, UUID).

boot_slave(Config) ->
    Path = filename:join(?config(priv_dir, Config), "boot_slave"),
    PathAlt = filename:join(?config(priv_dir, Config), "boot_slave_other"),
    UUID = uuid:get_v4(),
    ok = interclock:boot(root_boot, [{type, root}, {dir, Path},
                                     {uuid, UUID}]),
    {SeedId,_SeedEv} = itc:explode(itc:seed()),
    {UUID, SeedId} = interclock:id(root_boot),
    {UUID, Fork} = interclock:fork(root_boot), % returns the new fork to send away
    {UUID, Forked} = interclock:id(root_boot), % has its own forked id
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
    {UUID, good_id} = interclock:id(fork_recovery), % it recovered
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
    {UUID, good_id} = interclock:id(crashed_fork), % it recovered
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
    {UUID, good_id} = interclock:id(recovery_good), % it recovered
    %% The internal log should show no recovery needed
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{good_id, other_id}]},
     {booted, _, UUID, 1, []},
     {recovered, _, UUID, good_id, [failed_fork]},
     {booted, _, UUID, good_id, []} | _] = NewLog.

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
    ok = interclock:boot(fork_recovery, [{type, normal}, {dir, Path},
                                         {id, undefined}, {uuid, UUID}]),
    {UUID, good_id} = interclock:id(fork_recovery), % it recovered
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

%% TODO: - test join recoveries

%%%===================================================================
%%% Helpers
%%%===================================================================
disk_state(Db, Id, UUID) ->
    Ref = bitcask:open(Db, [read_write]),
    ok = bitcask:put(Ref, <<"id">>, term_to_binary(Id)),
    ok = bitcask:put(Ref, <<"uuid">>, term_to_binary(UUID)),
    ok = bitcask:close(Ref).

fake_log(List, UUID, LogFile) ->
    {ok, IoDevice} = file:open(LogFile, [append]),
    fake_log1(List, UUID, IoDevice).

fake_log1([], _, IoDevice) -> file:close(IoDevice);
fake_log1([{Type, Id, Terms}|Rest], UUID, IoDevice) ->
    io:format(IoDevice, "~p.~n", [{Type, calendar:local_time(), UUID, Id, Terms}]),
    fake_log1(Rest, UUID, IoDevice).
