import re


def validate_bucket_name(name):
    ''' Kontrollerer at bucketnavn valgt består av kun små bokstaver og tall, ord separert med '-', og at det ikke
        starter eller slutter med '-'

    :param name: String med bucketnavn som skal valideres
    '''

    valid_name_pattern = '(^[a-z0-9])([a-z0-9\-])+([a-z0-9])$'
    if not re.match(pattern=valid_name_pattern, string=name):
        raise NameError(f'Ulovlig bucketnavn ({name}): '
                        'Må være små bokstaver eller tall, ord separert med "-", og kan ikke starte eller ende med "-"')

def validate_datapackage_name(name):
    ''' Kontrollerer at pakkenavnet valgt består av kun små bokstaver og tall, ord separert med '-', og at det ikke
        starter eller slutter med '-'

    :param name: String med pakkenavn som skal valideres
    '''

    valid_name_pattern = '(^[a-z0-9])([a-z0-9\-])+([a-z0-9])$'
    if not re.match(pattern=valid_name_pattern, string=name):
        raise NameError(f'Ulovlig datapakkenavn ({name}): '
                        'Må være små bokstaver eller tall, ord separert med "-", og kan ikke starte eller ende med "-"')


def validate_cronjob_schedule(schedule):
    ''' Kontrollerer brukerinput for cronjob schedule
            Format på cronjob schedule string: "* * * * *"
            "minutt" "time" "dag i måned" "måned i år" "ukedag"
             (0-59)  (0-23)    (1-31)        (1-12)     (0-6)
    '''

    schedule_list = schedule.split(' ')

    if not len(schedule_list) is 5:
        raise ValueError(
            f'Schedule {schedule} har ikke riktig format. Må ha formatet: "<minutt> <time> <dag i måned> <måned> <ukedag>". '
            f'F.eks. "0 12 * * 2,4" vil gi: Hver tirsdag og torsdag kl 12.00 UTC')

    if not schedule_list[0] is '*':
        for minute in schedule_list[0].split(','):
            if not int(minute) in range(0, 60):
                raise ValueError(f'I schedule {schedule} er ikke {minute}'
                                 f' en gyldig verdi for minutt innenfor time. Gyldige verdier er 0-59, eller *')

    if not schedule_list[1] is '*':
        for hour in schedule_list[1].split(','):
            if not int(hour) in range(0, 24):
                raise ValueError(f'I schedule {schedule} er ikke {hour}'
                                 f' en gyldig verdi for time. Gyldige verdier er 0-23, eller *')

    if not schedule_list[2] is '*':
        for day in schedule_list[2].split(','):
            if not int(day) in range(1, 32):
                raise ValueError(f'I schedule {schedule} er ikke {day}'
                                 f' en gyldig verdi for dag i måned. Gyldige verdier er 1-31, eller *')

    if not schedule_list[3] is '*':
        for month in schedule_list[3].split(','):
            if not int(month) in range(1, 13):
                raise ValueError(f'I schedule {schedule} er ikke {month}'
                                 f' en gyldig verdi for måned. Gyldige verdier er 1-12, eller *')

    if not schedule_list[4] is '*':
        for weekday in schedule_list[4].split(','):
            if not int(weekday) in range(0, 7):
                raise ValueError(f'I schedule {schedule} er ikke {weekday}'
                                 f' en gyldig verdi for ukedag. Gyldige verdier er 0-6 (søn-lør), eller *')