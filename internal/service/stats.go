package service

import "time"

type Period struct {
	From time.Time
	To   time.Time
}

type PeriodStats struct {
	Egress uint64
	Period Period
}

type Stats struct {
	PreviousMonth PeriodStats
	CurrentMonth  PeriodStats
	CurrentWeek   PeriodStats
	CurrentDay    PeriodStats
}

func NewStats(now time.Time) *Stats {
	// Calculate period boundaries
	currentYear, currentMonth, currentDay := now.Date()

	// Previous month: first day of previous month to last day of previous month
	previousMonthStart := time.Date(currentYear, currentMonth-1, 1, 0, 0, 0, 0, time.UTC)
	previousMonthEnd := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC).Add(-time.Second)

	// Current month: first day of current month to now
	currentMonthStart := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC)

	// Current week: start of week (Monday) to now
	weekday := now.Weekday()
	// Convert Sunday (0) to 7 for easier calculation
	if weekday == time.Sunday {
		weekday = 7
	}
	daysFromMonday := int(weekday) - 1
	currentWeekStart := time.Date(currentYear, currentMonth, currentDay-daysFromMonday, 0, 0, 0, 0, time.UTC)

	// Current day: start of day to now
	currentDayStart := time.Date(currentYear, currentMonth, currentDay, 0, 0, 0, 0, time.UTC)

	return &Stats{
		PreviousMonth: PeriodStats{
			Period: Period{
				From: previousMonthStart,
				To:   previousMonthEnd,
			},
		},
		CurrentMonth: PeriodStats{
			Period: Period{
				From: currentMonthStart,
				To:   now,
			},
		},
		CurrentWeek: PeriodStats{
			Period: Period{
				From: currentWeekStart,
				To:   now,
			},
		},
		CurrentDay: PeriodStats{
			Period: Period{
				From: currentDayStart,
				To:   now,
			},
		},
	}
}

func (s *Stats) Earliest() time.Time {
	return s.PreviousMonth.Period.From
}

func (s *Stats) AddEgress(egress uint64, when time.Time) {
	// Add the egress to the corresponding period(s)
	// Previous month
	if !when.Before(s.PreviousMonth.Period.From) && when.Before(s.CurrentMonth.Period.From) {
		s.PreviousMonth.Egress += egress
	}

	// Current month
	if !when.Before(s.CurrentMonth.Period.From) {
		s.CurrentMonth.Egress += egress
	}

	// Current week
	if !when.Before(s.CurrentWeek.Period.From) {
		s.CurrentWeek.Egress += egress
	}

	// Current day
	if !when.Before(s.CurrentDay.Period.From) {
		s.CurrentDay.Egress += egress
	}
}
