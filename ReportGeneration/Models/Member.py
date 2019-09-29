class Member:

    def __init__(self, name, member_id):
        # Personnel attributes
        self.name = name
        self.id = member_id

        # Duty shift attributes
        self.completed_duty_shifts = False
        self.total_duty_shifts = 0
        self.warning_level = ''

        # Monthly stat attributes
        self.standby_hours_month = 0
        self.incentive_month = 0
        self.ambulance_calls_month = 0
        self.engine_calls_month = 0
        self.chief_calls_month = 0

        # Yearly stat attributes
        self.standby_hours_year = 0
        self.incentive_year = 0
        self.ambulance_calls_year = 0
        self.engine_calls_year = 0
        self.chief_calls_year = 0

    def get_name(self) -> str:
        return self.name

    def get_id(self) -> int:
        return self.id

    def get_completed_duty_shifts(self) -> bool:
        return self.completed_duty_shifts

    def get_total_duty_shifts(self) -> int:
        return self.total_duty_shifts

    def get_warning_level(self) -> str:
        return self.warning_level

    def get_standby_hours_month(self) -> int:
        return self.standby_hours_month

    def get_incentive_month(self) -> int:
        return self.incentive_month

    def get_ambulance_calls_month(self) -> int:
        return self.ambulance_calls_month

    def get_engine_calls_month(self) -> int:
        return self.engine_calls_month

    def get_chief_calls_month(self) -> int:
        return self.chief_calls_month

    def get_standby_hours_year(self) -> int:
        return self.standby_hours_year

    def get_incentive_year(self) -> int:
        return self.incentive_year

    def get_ambulance_calls_year(self) -> int:
        return self.ambulance_calls_year

    def get_engine_calls_year(self) -> int:
        return self.engine_calls_year

    def get_chief_calls_year(self) -> int:
        return self.chief_calls_year

    def set_completed_duty_shifts(self, did_complete_duty_shifts: bool):
        self.completed_duty_shifts = did_complete_duty_shifts

    def set_total_duty_shifts(self, total_duty_shifts: int):
        self.total_duty_shifts = total_duty_shifts

    def set_warning_level(self, warning_level: str):
        self.warning_level = warning_level

    def set_standby_hours_month(self, standby_hours_month: int):
        self.standby_hours_month = standby_hours_month

    def set_incentive_month(self, incentive_month: int):
        self.incentive_month = incentive_month

    def set_ambulance_calls_month(self, ambulance_calls_month: int):
        self.ambulance_calls_month = ambulance_calls_month

    def set_engine_calls_month(self, engine_calls_month: int):
        self.engine_calls_month = engine_calls_month

    def set_chief_calls_month(self, chief_calls_month: int):
        self.chief_calls_month = chief_calls_month

    def set_standby_hours_year(self, standby_hours_year: int):
        self.standby_hours_year = standby_hours_year

    def set_incentive_year(self, incentive_year: int):
        self.incentive_year = incentive_year

    def set_ambulance_calls_year(self, ambulance_calls_year: int):
        self.ambulance_calls_year = ambulance_calls_year

    def set_engine_calls_year(self, engine_calls_year: int):
        self.engine_calls_year = engine_calls_year

    def set_chief_calls_year(self, chief_calls_year: int):
        self.chief_calls_year = chief_calls_year
