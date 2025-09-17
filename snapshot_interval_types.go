package rafty

// snapshotTestHook is used for injecting errors during unit testing
var snapshotTestHook func() error
