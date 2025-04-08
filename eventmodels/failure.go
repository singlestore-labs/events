package eventmodels

type OnFailure int

const (
	OnFailureDiscard    OnFailure = iota // failed messages will be dropped
	OnFailureBlock                       // failed messages will retry forever and block the consumer
	OnFailureRetryLater                  // failed messages will go to the dead letter queue and be retried
	OnFailureSave                        // failed messages will go to the dead letter queue and not be retried
)
