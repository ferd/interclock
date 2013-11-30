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
     recovery_recovery].

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
%%% Test cases
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
                                                            {uuid, UUID},
                                                            {dir, Path}]),
    {error, missing_identity} = interclock:boot(root_name, [{type, normal},
                                                            {id, 1},
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
              {recovered, good_id, [failed_fork]}], % the fork worked
             UUID,
             LogFile),
    %% Boot the db, with an good id
    ok = interclock:boot(fork_recovery, [{type, normal}, {dir, Path},
                                         {id, good_id}, {uuid, UUID}]),
    {UUID, good_id} = interclock:id(fork_recovery), % it recovered
    %% The internal log should show no recovery needed
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{good_id, other_id}]},
     {booted, _, UUID, 1, []},
     {recovered, _, UUID, good_id, [failed_fork]},
     {booted, _, UUID, good_id, []} | _] = NewLog.

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
                                         {id, 1}, {uuid, UUID}]),
    {UUID, good_id} = interclock:id(fork_recovery), % it recovered
    %% The internal log should show this
    {ok,NewLog} = file:consult(LogFile),
    [{booted, _, UUID, 1, []},
     {forked, _, UUID, 1, [{good_id, other_id}]},
     {booted, _, UUID, 1, []},
     {recovered, _, UUID, good_id, [failed_fork]},
     {booted, _, UUID, 1, []},
     {recovered, _, UUID, good_id, [recovery]} | _] = NewLog.

%% TODO: test recovery failures, test join recoveries

%%%%%%%%%%%%%%%
%%% HELPERS %%%
%%%%%%%%%%%%%%%
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
