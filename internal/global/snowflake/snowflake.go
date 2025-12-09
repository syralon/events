package snowflake

import (
	"fmt"
	"github.com/syralon/snowflake"
)

var instance *snowflake.Generator

func Setup(node int, options ...snowflake.Option) error {
	var err error
	instance, err = snowflake.New(node, options...)
	return err
}

func Next() int64 {
	if instance == nil {
		panic(fmt.Errorf("you must setup snowflake first before call Next"))
	}
	return instance.Next()
}
