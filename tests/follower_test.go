package tests

import "time"

func subTestFollower(g *testGroup) {
	g.regSubTest("follow", follower_follow_test)
	g.regSubTest("follow EX", follower_follow_EX_test)
}

// A follower catching up from the leader's AOF must see the leader's
// deadline, not one restarted at the time it replays the command.
func follower_follow_EX_test(mc *mockServer) error {
	leader, err := mockOpenServer(MockServerOptions{Silent: true, Metrics: false})
	if err != nil {
		return err
	}
	defer leader.Close()
	follower, err := mockOpenServer(MockServerOptions{Silent: true, Metrics: false})
	if err != nil {
		return err
	}
	defer follower.Close()
	if err := leader.DoBatch(
		Do("SET", "fleet", "truck1", "EX", 60, "POINT", 10, 10).OK(),
	); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	return follower.DoBatch(
		Do("FOLLOW", "localhost", leader.port).OK(),
		Sleep(time.Second/2),
		Do("TTL", "fleet", "truck1").Func(ttlWithin(56, 58)),
	)
}

func follower_follow_test(mc *mockServer) error {
	mc2, err := mockOpenServer(MockServerOptions{
		Silent: true, Metrics: false,
	})
	if err != nil {
		return err
	}
	defer mc2.Close()
	err = mc.DoBatch(
		Do("SET", "mykey", "truck1", "POINT", 10, 10).OK(),
		Do("SET", "mykey", "truck2", "POINT", 10, 10).OK(),
		Do("SET", "mykey", "truck3", "POINT", 10, 10).OK(),
		Do("SET", "mykey", "truck4", "POINT", 10, 10).OK(),
		Do("SET", "mykey", "truck5", "POINT", 10, 10).OK(),
		Do("SET", "mykey", "truck6", "POINT", 10, 10).OK(),
		Do("CONFIG", "SET", "requirepass", "1234").OK(),
		Do("AUTH", "1234").OK(),
	)
	if err != nil {
		return err
	}
	err = mc2.DoBatch(
		Do("SET", "mykey2", "truck1", "POINT", 10, 10).OK(),
		Do("SET", "mykey2", "truck2", "POINT", 10, 10).OK(),
		Do("GET", "mykey2", "truck1").Str(`{"type":"Point","coordinates":[10,10]}`),
		Do("GET", "mykey2", "truck2").Str(`{"type":"Point","coordinates":[10,10]}`),

		Do("CONFIG", "SET", "leaderauth", "1234").OK(),
		Do("FOLLOW", "localhost", mc.port).OK(),
		Do("GET", "mykey", "truck1").Err("catching up to leader"),
		Sleep(time.Second/2),

		Do("GET", "mykey", "truck1").Err(`{"type":"Point","coordinates":[10,10]}`),
		Do("GET", "mykey", "truck2").Err(`{"type":"Point","coordinates":[10,10]}`),
	)
	if err != nil {
		return err
	}

	err = mc.DoBatch(
		Do("SET", "mykey", "truck7", "POINT", 10, 10).OK(),
		Do("SET", "mykey", "truck8", "POINT", 10, 10).OK(),
		Do("SET", "mykey", "truck9", "POINT", 10, 10).OK(),
	)
	if err != nil {
		return err
	}

	err = mc2.DoBatch(
		Sleep(time.Second/2),
		Do("GET", "mykey", "truck7").Str(`{"type":"Point","coordinates":[10,10]}`),
		Do("GET", "mykey", "truck8").Str(`{"type":"Point","coordinates":[10,10]}`),
		Do("GET", "mykey", "truck9").Str(`{"type":"Point","coordinates":[10,10]}`),
	)
	if err != nil {
		return err
	}

	return nil
}
