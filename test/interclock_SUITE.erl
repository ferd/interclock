-module(interclock_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [boot_root, boot_slave].

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
    ok = interclock:boot(root_name, [{type, root}, {dir, Path}]),
    {SeedId,_SeedEv} = itc:explode(itc:seed()),
    {UUID, SeedId} = interclock:id(root_name),
    %% Same name / type / path
    {error, exists} = interclock:boot(root_name, [{type, root}, {dir, Path}]),
    %% No id sent with a non-root, means we fail.
    {error, missing_id} = interclock:boot(root_name, [{type, normal}, {dir, Path}]),
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
    ok = interclock:boot(root_other, [{type, root}, {dir, PathAlt}]),
    {UUIDOther, SeedId} = interclock:id(root_other),
    ?assertNotEqual(UUIDOther, UUID).

boot_slave(Config) ->
    Path = filename:join(?config(priv_dir, Config), "boot_slave"),
    PathAlt = filename:join(?config(priv_dir, Config), "boot_slave_other"),
    ok = interclock:boot(root_boot, [{type, root}, {dir, Path}]),
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

