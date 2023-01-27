package errorFunc

import (
	"fmt"
)

func ErrorFormated(errorMessage string) string {
	return fmt.Sprintf("You receveid the following error: %s", errorMessage)
}
