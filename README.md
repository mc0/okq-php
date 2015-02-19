# okq-php

A PHP driver for the [okq](https://github.com/mc0/okq) persistent queue.

okq uses the redis protocol and calling conventions as it's interface, so any
standard redis client can be used to interact with it. This package wraps around
a normal redis driver, however, to provide a convenient interface to interact
with. Specifically, it creates a simple interface for event consumers to use so
they retrieve events through a channel rather than implementing that logic
manually every time.

## Usage

Commands for okQ are implemented on the `Okq` class parameters resemble those
of the okQ command itself. The `Okq` class extends [phpredis/Redis](https://github.com/phpredis/phpredis)
and can be constructed using the same arguments:

```PHP
$q = new Okq();
$q->connect('127.0.0.1', 4777);
```

Then to add jobs you just run the command!

```PHP
$event = array('test' => 1);
$eventId = '233'; // this can also be null and will be generated
$success = $q->qlpush('testQueue', json_encode($event), $eventId);
```

If you're a consumer you can just use the convenience `consume` method.

```PHP
/* the $callback can be any callable:
 - array('StaticClass', 'funcName')
 - array($obj, 'funcName')
 - 'funcName'
 - create_function('$event', '...')
*/
$callback = function ($event) use ($q) {
    // do anything or nothing at all
    return Okq::STATUS_ACK;
};
$timeout = 30;
$q->consume($callback, array('testQueue'), $timeout);
```
