import random
import string

from fastapi import APIRouter, Body
from environs import Env
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi.openapi.docs import get_redoc_html
from starlette.responses import JSONResponse

import dbs_assignment.schemas

# from fastapi.openapi.docs import get_openapi

# Version of a fulfilled second assessment, first third assessment
env = Env()
env.read_env()
conn = psycopg2.connect(database=env("DATABASE_NAME"), user=env("DATABASE_USER"),
                        password=env("DATABASE_PASSWORD"), host=env("DATABASE_HOST"),
                        port=env("DATABASE_PORT"))
conn.autocommit = True
router = APIRouter()


@router.get("/v1/status")
async def status():
    local_cursor = conn.cursor()
    local_cursor.execute('SELECT version();')
    test = local_cursor.fetchall()
    print(test[0][0])
    return {
        'version': test
    }


@router.get("/v1/passengers/{passenger_id}/companions")
def companions(passenger_id: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
    select t.passenger_id as id,
    t.passenger_name as name,
    count(tf.flight_id) as flights_count,
    array_agg(tf.flight_id order by tf.flight_id asc) AS flights
    from tickets t
    join ticket_flights tf on t.ticket_no = tf.ticket_no
    where tf.flight_id in (
        select tf.flight_id
        from ticket_flights tf
        join tickets t ON t.ticket_no = tf.ticket_no
        where t.passenger_id = \'''' + passenger_id + '''\'
    ) and t.passenger_id <> \'''' + passenger_id + '''\'
    group by t.passenger_id,
    t.passenger_name, t.ticket_no
    order by count(tf.flight_id) desc, t.passenger_id asc;
    ''')
    exe = local_cursor.fetchall()
    return {
        "results": exe
    }


@router.get("/v1/bookings/{booking_id}")
def booking_detail(booking_id: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute(
        'select b.book_ref,'
        ' b.book_date,'
        ' t.ticket_no,'
        ' t.passenger_id,'
        ' t.passenger_name,'
        ' f.flight_no, '
        'bp.seat_no,'
        ' f.aircraft_code,'
        ' f.arrival_airport,'
        'f.departure_airport,'
        ' f.scheduled_arrival, '
        'f.scheduled_departure,'
        ' bp.boarding_no'
        ' from bookings b'
        ' join tickets t on b.book_ref = t.book_ref'
        ' join boarding_passes bp on t.ticket_no  = bp.ticket_no '
        'join flights f on bp.flight_id = f.flight_id '
        'where b.book_ref =\'' + booking_id + '\' order by t.ticket_no , bp.boarding_no;')
    exe = local_cursor.fetchall()
    passes = []
    for record in exe:
        passes.append({
            "id": record['ticket_no'],
            "passenger_id": record['passenger_id'],
            "passenger_name": record['passenger_name'],
            "boarding_no": record['boarding_no'],
            "flight_no": record['flight_no'],
            "seat": record['seat_no'],
            "aircraft_code": record['aircraft_code'],
            "arrival_airport": record['arrival_airport'],
            "departure_airport": record['departure_airport'],
            "scheduled_arrival": record['scheduled_arrival'],
            "scheduled_departure": record['scheduled_departure']
        })
    return {
        "result": {
            "id": exe[0]['book_ref'],
            "book_date": exe[0]['book_date'],
            "boarding_passes": passes
        }
    }


@router.get("/v1/flights/late-departure/{delay}")
def late_departure(delay: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
    select f.flight_id,
    f.flight_no,
    (extract(hour from (f.actual_departure - f.scheduled_departure))*60
    + extract(minute from f.actual_departure - f.scheduled_departure)) as delay
    from flights f
    where (extract(hour from (f.actual_departure - f.scheduled_departure))*60
    + extract(minute from f.actual_departure - f.scheduled_departure)) >''' + delay +
                         ''' and f.status = 'Arrived'
                         order by delay desc, flight_id;''')
    exe = local_cursor.fetchall()
    return {
        "results": exe
    }


@router.get("/v1/top-airlines")
def top_airlines(limit: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
    select f.flight_no,
    count(*) as count
    from flights f
    join ticket_flights tf on f.flight_id = tf.flight_id
    where f.status = 'Arrived'
    group by flight_no
    order by count
    desc limit
    ''' + limit + ';')
    exe = local_cursor.fetchall()
    return {
        "results": exe
    }


@router.get("/v1/departures")
def scheduled_flights(airport: str, day: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
    select f.flight_id, f.flight_no,
    f.scheduled_departure from flights f
    join airports_data ad on
    f.departure_airport = ad.airport_code
    where extract(isodow from f.scheduled_departure) =''' + day + '''
    and ad.airport_code = \'''' + airport + '''\' and f.status = 'Scheduled'
    order by scheduled_departure, flight_id;''')
    exe = local_cursor.fetchall()
    return {
        "results": exe
    }


@router.get("/test")
def testing():
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''select distinct flights.flight_id
    from flights
    order by flights.flight_id desc''')
    exe = local_cursor.fetchall()
    records = []
    for number in exe:
        records.append(number['flight_id'])
    return records


def generateId():
    localBool = False
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''select distinct flights.flight_id
    from flights
    order by flights.flight_id desc''')
    exe = local_cursor.fetchall()
    records = []
    for number in exe:
        records.append(number['flight_id'])
    maximum = max(records)
    while 1:
        selfish = random.randint(0, maximum)
        if records.__contains__(selfish) is False:
            return selfish


def generateNo():
    return random.choice(string.ascii_letters) + random.choice(string.ascii_letters) + str(random.randint(999, 10000))


@router.get("/vagh/flight/{flight_id}", status_code=201)
def get_flight(flight_id: int):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    try:
        local_cursor.execute('''select * from flights f where f.flight_id = \'''' + str(flight_id) + '''\';''')
        exe = local_cursor.fetchall()
        return exe[0]
    except:
        return "Could not retrieve from a db"


@router.post("/vagh/flight", status_code=201)
def create_flight(payload: dbs_assignment.schemas.Flight):
    if payload.flight_id is None:
        payload.flight_id = generateId()
    if payload.flight_no is None:
        payload.flight_no = generateNo()
    if payload.status is None:
        payload.status = "Scheduled"
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    try:
        local_cursor.execute('''insert into flights(flight_id, flight_no,
        scheduled_departure, scheduled_arrival, actual_departure,
        departure_airport, arrival_airport, status, aircraft_code) values(\'''' + str(payload.flight_id) +
                             '''\', \'''' + payload.flight_no + '''\',\'''' + payload.scheduled_departure +
                             '''\',\'''' + payload.scheduled_arrival + '''\',\'''' + payload.scheduled_departure +
                             '''\',\'''' + payload.departure_airport + '''\',\'''' +
                             payload.arrival_airport + '''\',\'''' + payload.status + '''\',\'''' + payload.aircraft_code + '''\');''')
        return {"flight_id": payload.flight_id}
    except:
        return "Could not insert to the db"


@router.patch("/vagh/flight/{flight_id}", status_code=200)
def update_flight(flight_id: str, payload: dict = Body(...)):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    try:
        column_name = list(payload.keys())[0]
        local_cursor.execute('''
        update flights set ''' + column_name + ''' = \'''' + payload[
            column_name] + '''\' where flight_id = \'''' + flight_id + '''\';''')
        return "Successfully updated row"
    except:
        return "could not update the db"


@router.delete("/vagh/flight/{flight_id}", status_code=200)
def delete_flight(flight_id: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    try:
        local_cursor.execute('DELETE FROM flights WHERE flight_id = %s;', (flight_id,))
        if local_cursor.rowcount == 0:
            return JSONResponse(status_code=404, content={"detail": "Flight not found"})
        return JSONResponse(status_code=200, content={"detail": "Flight deleted successfully"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": "Unable to delete flight", "error": str(e)})
    finally:
        local_cursor.close()


@router.get("/v1/airports/{airport}/destinations")
def destinations(airport: str):
    local_cursor = conn.cursor()
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
    select distinct f.arrival_airport
     from flights f
     where f.departure_airport = \'''' + airport + '''\'
     order by f.arrival_airport asc;''')
    exe = local_cursor.fetchall()
    results = []
    for record in exe:
        results.append(record[0])
    return {
        "results": results
    }


@router.get("/v1/airlines/{flight_no}/load")
def flight_load(flight_no: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
    select f.flight_id as id,
    count(distinct s.seat_no) as aircraft_capacity,
    count(distinct tf.ticket_no) as load,
    case when round(count(distinct tf.ticket_no)::numeric
    / count(distinct s.seat_no) * 100, 2) = 100 then 100
    else round(count(distinct tf.ticket_no)::numeric
    / count(distinct s.seat_no) * 100, 2)
    END AS percentage_load
    from flights f
    join seats s on f.aircraft_code = s.aircraft_code
    join ticket_flights tf on f.flight_id = tf.flight_id
    where f.flight_no = \'''' + flight_no + '''\'group by f.flight_id;''')
    exe = local_cursor.fetchall()
    return {
        "results": exe
    }


@router.get("/v1/airlines/{flight_no}/load-week")
def load_week(flight_no: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
     select count(distinct tf.ticket_no)::numeric /
     count(distinct s.seat_no) * 100 as percentage_load,
     f.scheduled_departure
     from flights f
     join seats s on f.aircraft_code = s.aircraft_code
     join ticket_flights tf on f.flight_id = tf.flight_id
     where flight_no = \'''' + flight_no + '''\'
     group by extract(isodow from f.scheduled_departure),
     f.flight_id;'''
                         )
    exe = local_cursor.fetchall()
    result = [[], [], [], [], [], [], []]
    for record in exe:
        result[record["scheduled_departure"].isoweekday() - 1].append(record["percentage_load"])
    for i in range(len(result)):
        sum = 0
        for value in result[i]:
            sum += value
        result[i] = round(sum / len(result[i]), 2)
    return {
        "result": {
            "flight_no": flight_no,
            "monday": result[0],
            "tuesday": result[1],
            "wednesday": result[2],
            "thursday": result[3],
            "friday": result[4],
            "saturday": result[5],
            "sunday": result[6]
        }
    }


@router.get("/v3/aircrafts/{aircraft_code}/seats/{seat_choice}")
def seat_choice(aircraft_code: str, seat_choice: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
    select subq.seat, count(subq.seat) as count
    from (
        select bp.seat_no as seat,
        dense_rank() over (partition by bp.flight_id
        order by b.book_date) as seat_rank
        from tickets t
        join boarding_passes bp on bp.ticket_no = t.ticket_no
        join bookings b on t.book_ref = b.book_ref
        join flights f on f.flight_id = bp.flight_id
        where f.aircraft_code = \'''' + aircraft_code + '''\') subq
    where subq.seat_rank = \'''' + seat_choice + '''\'
    group by seat
    order by count desc
    limit 1;''')
    exe = local_cursor.fetchall()
    return {
        "result": exe[0]
    }


def makeCurrArr(curr_ticket_no, curr_pass_name):
    return {
        "ticket_no": curr_ticket_no,
        "passenger_name": curr_pass_name,
        "flights": []
    }


@router.get("/v3/air-time/{book_ref}")
def air_time(book_ref: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
        select t.ticket_no,
        t.passenger_name, f.departure_airport,
        f.arrival_airport,
        to_char(f.actual_arrival - f.actual_departure,
        'fmhh24:mi:ss') as flight_time,
        to_char(sum(f.actual_arrival - f.actual_departure)
        over (partition by t.ticket_no
        rows between unbounded preceding and current row),
        'fmhh24:mi:ss') as total_time
        from tickets t
        join ticket_flights tf on t.ticket_no = tf.ticket_no
        join flights f on tf.flight_id = f.flight_id
        where t.book_ref = \'''' + book_ref + '''\'
        group by t.ticket_no, f.actual_departure, f.actual_arrival,
        f.departure_airport, f.arrival_airport;
    ''')
    exe = local_cursor.fetchall()
    curr_ticket_no = ""
    results = []
    for i in range(0, len(exe)):
        if curr_ticket_no != exe[i]['ticket_no']:
            if i != 0:
                results.append(curr_arr)
            curr_ticket_no = exe[i]['ticket_no']
            curr_pass_name = exe[i]['passenger_name']
            curr_arr = makeCurrArr(curr_ticket_no, curr_pass_name)
        curr_arr['flights'].append({
            "departure_airport": exe[i]['departure_airport'],
            "arrival_airport": exe[i]['arrival_airport'],
            "flight_time": exe[i]['flight_time'],
            "total_time": exe[i]['total_time']
        })
    results.append(curr_arr)
    return {
        "results": results
    }


@router.get("/v3/airlines/{flight_no}/top_seats")
def top_seats(flight_no: str, limit: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
    with sd as (
        select f.flight_id, s.seat_no as seat,
        case when bp.seat_no is not null then 1 else 0 end as "is_filled",
        row_number() over (partition by s.seat_no order by f.flight_id) as sf_num,
        row_number() over (partition by s.seat_no, case when bp.seat_no
        is not null then 1 else 0 end order by f.flight_id) as sf_with_occup
        from flights f
        join seats s on f.aircraft_code = s.aircraft_code
        left join boarding_passes bp on f.flight_id = bp.flight_id and s.seat_no = bp.seat_no
        where f.flight_no = \'''' + flight_no + '''\'
        order by f.flight_id
    )
    select sd.seat, count(sd.flight_id) as flights_count,
    array_agg(sd.flight_id order by sd.flight_id) as flights
    from sd
    where sd."is_filled" = 1
    group by sd.sf_num - sd.sf_with_occup, sd.seat
    order by flights_count desc, sd.seat
    limit ''' + limit + ''';
    ''')
    exe = local_cursor.fetchall()
    return {
        "results": exe
    }


@router.get("/v3/aircrafts/{aircraft_code}/top-incomes")
def top_incomes(aircraft_code: str):
    local_cursor = conn.cursor(cursor_factory=RealDictCursor)
    local_cursor.execute('SET search_path TO \'bookings\';')
    local_cursor.execute('''
    select subq.total_amount as total_amount,
    concat(subq.yearr, '-', subq.monthh) as month,
    subq.dayy as day from (
        select sum(tf.amount) as total_amount,
        extract(year from f.actual_departure) as yearr,
        extract(month from f.actual_departure) as monthh,
        extract(day from f.actual_departure) as dayy,
        rank() over (partition by extract(year from f.actual_departure),
        extract(month from f.actual_departure) order by sum(tf.amount) desc) as row_no
        from flights f
        join ticket_flights tf on tf.flight_id = f.flight_id
        where f.aircraft_code = \'''' + aircraft_code + '''\' and f.actual_departure is not null
        group by extract(year from f.actual_departure),
        extract(month from f.actual_departure), extract(day from f.actual_departure)
    ) subq
    where subq.row_no = 1
    group by month, subq.dayy, total_amount
    order by total_amount desc;
    ''')
    exe = local_cursor.fetchall()
    results = []
    for record in exe:
        results.append({
            "total_amount": int(record['total_amount']),
            "month": record['month'],
            "day": str(int(record['day']))
        })
    return {
        "results": results
    }


@router.get("/")
async def get_documentation():
    return get_redoc_html(openapi_url="/openapi.json", title="Your API Documentation")
