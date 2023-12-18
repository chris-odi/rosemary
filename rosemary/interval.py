from sqlalchemy import func


class RosemaryInterval:

    def __init__(
            self,
            seconds: int = 0,
            minutes: int = 0,
            hours: int = 0,
    ):
        self.seconds = seconds
        self.minutes = minutes
        self.hours = hours

    def get_interval(self) -> int:
        return func.make_interval(0, 0, 0, 0, self.hours, self.minutes, self.seconds)

    def get_datetime_plus_interval(self):
        return self.get_interval() + func.now()
