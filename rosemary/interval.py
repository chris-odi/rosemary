from sqlalchemy import func


class RosemaryInterval:

    def __init__(
            self,
            seconds: int = 0,
            minutes: int = 0,
            hours: int = 0,
            days: int = 0,
            weeks: int = 0,
            months: int = 0,
            years: int = 0
    ):
        self.seconds = seconds
        self.minutes = minutes
        self.hours = hours
        self.days = days
        self.weeks = weeks
        self.months = months
        self.years = years

    def get_interval(self) -> int:
        return func.make_interval(
            self.years, self.months, self.weeks, self.days, self.hours, self.minutes, self.seconds
        )

    def get_datetime_plus_interval(self):
        return self.get_interval() + func.now()
