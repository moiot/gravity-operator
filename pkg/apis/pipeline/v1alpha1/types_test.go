package v1alpha1

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestIsInScheduledPausePeriod(t *testing.T) {
	r := require.New(t)

	shanghaiLoc, err := time.LoadLocation("Asia/Shanghai")
	r.NoError(err)

	newYorkLoc, err := time.LoadLocation("America/New_York")
	r.NoError(err)

	var cases = []struct{
		name string
		PeriodSpec  ScheduledPausePeriod
		CurrentTime time.Time
		expected    bool
	}{
		{
			name: "case 1",
			PeriodSpec:  ScheduledPausePeriod{StartTime: "0 0 6 * * *", EndTime: "0 0 10 * * *", Location: "Asia/Shanghai"},
			CurrentTime: time.Date(2019, 1, 16, 5, 0, 0, 0, shanghaiLoc),
			expected:    false,
		},
		{
			name: "case 2",
			PeriodSpec:  ScheduledPausePeriod{StartTime: "0 0 6 * * *", EndTime: "0 0 10 * * *", Location: "Asia/Shanghai"},
			CurrentTime: time.Date(2019, 1, 16, 7, 0, 0, 0, shanghaiLoc),
			expected:    true,
		},
		{
			name: "case 3",
			PeriodSpec:  ScheduledPausePeriod{StartTime: "0 0 6 * * *", EndTime: "0 0 10 * * *", Location: "Asia/Shanghai"},
			CurrentTime: time.Date(2019, 1, 15, 7, 0, 0, 0, shanghaiLoc),
			expected:    true,
		},
		// The same as above, just use a different timezone
		{
			name: "case 4",
			PeriodSpec:  ScheduledPausePeriod{StartTime: "0 0 6 * * *", EndTime: "0 0 10 * * *", Location: "Asia/Shanghai"},
			CurrentTime: time.Date(2019, 1, 16, 16, 0, 0, 0, newYorkLoc),
			expected:    false,
		},
		{
			name: "case 5",
			PeriodSpec:  ScheduledPausePeriod{StartTime: "0 0 6 * * *", EndTime: "0 0 10 * * *", Location: "Asia/Shanghai"},
			CurrentTime: time.Date(2019, 1, 16, 18, 0, 0, 0, newYorkLoc),
			expected:    true,
		},
		{
			name: "case 6",
			PeriodSpec:  ScheduledPausePeriod{StartTime: "0 0 6 * * *", EndTime: "0 0 10 * * *", Location: "Asia/Shanghai"},
			CurrentTime: time.Date(2019, 1, 15, 18, 0, 0, 0, newYorkLoc),
			expected:    true,
		},

	}

	for _, c := range cases {
		r.Equal(c.expected, IsInScheduledPausePeriod(c.PeriodSpec, c.CurrentTime), c.name)
	}
}
