package eventmodels

import (
	"database/sql/driver"

	googleuuid "github.com/google/uuid"

	heliosuuid "singlestore.com/helios/uuid"
)

// BinraryEventID is a uuid type that presents itself the the sql driver as binary bytes.
// This is compatible with SingleStore
type BinaryEventID struct {
	googleuuid.UUID
}

// NewBinaryEventID generates a random event id
func (u BinaryEventID) New() BinaryEventID { return BinaryEventID{UUID: googleuuid.New()} }

var _ AbstractID[BinaryEventID] = &BinaryEventID{}

func (u BinaryEventID) Value() (driver.Value, error) {
	return u.UUID.MarshalBinary()
}

// StringEventUUID is a UUID type that presents itself to the sql driver as a string
// This is compatible with PostgreSQL
type StringEventID struct {
	heliosuuid.ObjectID
}

func (u StringEventID) New() StringEventID { return heliosuuid.New[StringEventID]() }

var _ AbstractID[StringEventID] = &StringEventID{}
