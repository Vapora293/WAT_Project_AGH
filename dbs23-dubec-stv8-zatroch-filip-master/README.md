#### Slovenská technická univerzita v Bratislave
#### Fakulta informatiky a informačných technológií
### 2022/2023
# Database Systems
### Assignment 1 & 2 & 3
### Filip Zatroch
#### Supervisor: Ing. Jakub Dubec
#### Time: Thursday 8:00
#### Date: 21.3.2023
This is the repositary of DBS assignment (currently third) of Filip Zatroch. Here you can find all the information
about endpoints located in the dbs-assignment folder. Program is implemented using python 3.11, psycopg2 and fastAPI
libs.

## Content of the repository
### Assignment 3
Application contains all the endpoints that are neccessary to complete the assignment.
#### Seat Choice - `GET v3/aircrafts/:aircraft_code/seats/:seat_choice`
Query is defined by a formatted subquery. At first, dense_rank function makes their own rankings for each flight_id
that has been operated by the defined aircraft based on the time the seat has been booked. Then, we filter records
which were not booked in the desired place. And then their occurence is counted based on a seat_no. The seat_no with
the highest occurence gets returned by the query. It is quite chunky and needs to do a lot of computations since it
ranks the seats for all the flight_ids that match desired aircraft, therefore it is slower than other queries.
```python
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
```

##### Results for `http://localhost:8000/v1/passengers/5260%20236351/companions`:
```json
{
	"results": {
		"seat": "19A",
		"count": 679
	}
}
```

#### Air Time - `GET /v3/air-time/:book_ref`
Air time query returns not yet properly formatted result for the json. It returns records
with t.ticket_no and t_passenger name and they are grouped by to a [] in code. The total
time is counted based on a same ticket_no meaning all the rows that preceed the current one
are summed up. Only the tickets that belong to the same book_ref are returned. 
```python
@router.get("/v3/air-time/{book_ref}")
def seat_choice(book_ref: str):
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
```

##### Results for `http://localhost:8000/v3/air-time/0000E0`:
```json
{
	"results": [
		{
			"ticket_no": "0005435467934",
			"passenger_name": "IRINA ALEKSEEVA",
			"flights": [
				{
					"departure_airport": "UUS",
					"arrival_airport": "SVO",
					"flight_time": "8:40:00",
					"total_time": "8:40:00"
				},
				{
					"departure_airport": "SVO",
					"arrival_airport": "UUS",
					"flight_time": "8:39:00",
					"total_time": "17:19:00"
				}
			]
		},
		{
			"ticket_no": "0005435467935",
			"passenger_name": "LYUDMILA NOVIKOVA",
			"flights": [
				{
					"departure_airport": "UUS",
					"arrival_airport": "SVO",
					"flight_time": "8:40:00",
					"total_time": "8:40:00"
				},
				{
					"departure_airport": "SVO",
					"arrival_airport": "UUS",
					"flight_time": "8:39:00",
					"total_time": "17:19:00"
				}
			]
		}
	]
}
```
#### Top Seats - `GET /v3/airlines/:flight_no/top_seats?limit=:limit`
This query uses subquery that is returned in order to properly format the result. It is solved
based on a problem of gaps and islands. We define a column is_filled which compares with left join
whether seats that actually are on an aircraft (the ones from s.seats that belong to the same 
aircraft_code) are also present under the selected flight_id. If not, bp.seat_no appears null meaning
we can work with this param later. First row number counts the occurence no matter if the seat has
been filled or not, meaning for each seat you get in the end the same number - the number of flight_id
that match desired conditions. The second row increments only when the seat is occupied.
Then, in the outer query, we filter only the rows that say that the seat has been filled. Group by
statement groups the results based on the "islands" meaning that on a flight no.25 where the seat has
been occupied up to now 14 times the result will be 11. On the next flight no.26 the seat is occupied again,
the result will still be 11. On the third one the same, fourth one the same. On the fifth flight the flight is
not occupied, won't appear in these records, but the next occupied flight will get different number because the
count for the occupied seats hasn't incremented once, meaning the result will be 12. All these groups are counted
and the seats with the highest count get returned and the number of returned records is limited to the desired
number from query.
```python
@router.get("/v3/airlines/{flight_no}/top_seats")
def seat_choice(flight_no: str, limit: str):
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
```

##### Results for `http://localhost:8000/v3/airlines/PG0055/top_seats?limit=3`:
```json
{
	"results": [
		{
			"seat": "2A",
			"flights_count": 22,
			"flights": [
				4492,
				4493,
				4495,
				4496,
				4499,
				4501,
				4502,
				4506,
				4509,
				4510,
				4515,
				4516,
				4518,
				4519,
				4520,
				4521,
				4523,
				4525,
				4526,
				4530,
				4531,
				4532
			]
		},
		{
			"seat": "1A",
			"flights_count": 18,
			"flights": [
				4605,
				4607,
				4610,
				4611,
				4614,
				4615,
				4618,
				4619,
				4621,
				4624,
				4625,
				4628,
				4631,
				4633,
				4636,
				4638,
				4641,
				4642
			]
		},
		{
			"seat": "1B",
			"flights_count": 18,
			"flights": [
				4922,
				4923,
				4924,
				4926,
				4927,
				4931,
				4932,
				4936,
				4938,
				4940,
				4941,
				4944,
				4946,
				4948,
				4952,
				4953,
				4957,
				4959
			]
		}
	]
}
```
#### Top Incomes - `GET /v3/aircrafts/:aircraft_code/top-incomes`
Top incomes query is performed by a subquery. We do rankings for all the flight_id that match
desired aircraft_code. Ranking is evaluated for each year + month + day separately, we count all the
money that has been spend from tf.amount on that day and we rank the sums for each year + month
separately. From the subquery we choose just the records that match the best day of each month.
The formatting is done in the query for year + month but the int type for the amount that has been spend
in a day is formatted in the code.
```python
@router.get("/v3/aircrafts/{aircraft_code}/top-incomes")
def seat_choice(aircraft_code: str):
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
```

##### Results for `http://localhost:8000/v3/aircrafts/319/top-incomes`:
```json
{
	"results": [
		{
			"total_amount": 66826900,
			"month": "2017-1",
			"day": "22"
		},
		{
			"total_amount": 65372700,
			"month": "2017-2",
			"day": "25"
		},
		{
			"total_amount": 65233200,
			"month": "2016-12",
			"day": "18"
		},
		{
			"total_amount": 65204500,
			"month": "2016-11",
			"day": "5"
		},
		{
			"total_amount": 65049000,
			"month": "2016-10",
			"day": "16"
		},
		{
			"total_amount": 65029800,
			"month": "2017-5",
			"day": "21"
		},
		{
			"total_amount": 64915900,
			"month": "2017-7",
			"day": "30"
		},
		{
			"total_amount": 64707700,
			"month": "2017-6",
			"day": "3"
		},
		{
			"total_amount": 64691700,
			"month": "2016-9",
			"day": "25"
		},
		{
			"total_amount": 64020500,
			"month": "2017-4",
			"day": "1"
		},
		{
			"total_amount": 63784200,
			"month": "2017-3",
			"day": "19"
		},
		{
			"total_amount": 63569800,
			"month": "2017-8",
			"day": "6"
		},
		{
			"total_amount": 61256300,
			"month": "2016-8",
			"day": "28"
		}
	]
}
```



### Assignment 2
Application contains all the endpoints that are neccessary to complete the assignment.
#### Companions - `GET /v1/passengers/:passenger_id/companions`
Endpoint returns the passengers that have been travelling on the same flights as the mentioned passenger. 
Query consists of selecting desired properties, counting the number of return flights and aggregate of an array of flight ids.
Then there is a nested select, that returns all the flights that are associated with typed passenger_id. Results are excluded of
a passenger_id we have typed and ordered by the number of flights and passenger_id.
```python
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
```
##### Results for `http://localhost:8000/v1/passengers/5260%20236351/companions`:
```json
{
	"results": [
		{
			"id": "0725 420471",
			"name": "VLADIMIR BARANOV",
			"flights_count": 2,
			"flights": [
				36747,
				99516
			]
		},
		{
			"id": "0775 008320",
			"name": "YURIY GRIGOREV",
			"flights_count": 2,
			"flights": [
				36747,
				99516
			]
		},
		{
			"id": "7138 903879",
			"name": "ALEKSEY KUZMIN",
			"flights_count": 2,
			"flights": [
				36747,
				99516
			]
		},...
    ]
}
```

#### Booking Detail `GET /v1/bookings/:booking_id`
Endpoint returns all info about the booking. At first desired columns are selected and two joins are performed
while the result is ordered by ticket_no and boarding_no.
```python
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
```
##### Results for `http://localhost:8000/v1/bookings/000067`:
```json
{
	"result": {
		"id": "000067",
		"book_date": "2016-08-11T18:36:00+00:00",
		"boarding_passes": [
			{
				"id": "0005434482035",
				"passenger_id": "1361 389085",
				"passenger_name": "ANNA CHERNOVA",
				"boarding_no": 8,
				"flight_no": "PG0156",
				"seat": "2A",
				"aircraft_code": "CR2",
				"arrival_airport": "NJC",
				"departure_airport": "LED",
				"scheduled_arrival": "2016-08-24T15:30:00+00:00",
				"scheduled_departure": "2016-08-24T11:55:00+00:00"
			},
			{
				"id": "0005434482035",
				"passenger_id": "1361 389085",
				"passenger_name": "ANNA CHERNOVA",
				"boarding_no": 8,
				"flight_no": "PG0157",
				"seat": "5D",
				"aircraft_code": "CR2",
				"arrival_airport": "LED",
				"departure_airport": "NJC",
				"scheduled_arrival": "2016-08-29T15:25:00+00:00",
				"scheduled_departure": "2016-08-29T11:50:00+00:00"
			},
			{
				"id": "0005434482036",
				"passenger_id": "8193 811215",
				"passenger_name": "MAKSIM BORISOV",
				"boarding_no": 6,
				"flight_no": "PG0157",
				"seat": "22D",
				"aircraft_code": "CR2",
				"arrival_airport": "LED",
				"departure_airport": "NJC",
				"scheduled_arrival": "2016-08-29T15:25:00+00:00",
				"scheduled_departure": "2016-08-29T11:50:00+00:00"
			},
			{
				"id": "0005434482036",
				"passenger_id": "8193 811215",
				"passenger_name": "MAKSIM BORISOV",
				"boarding_no": 7,
				"flight_no": "PG0156",
				"seat": "1C",
				"aircraft_code": "CR2",
				"arrival_airport": "NJC",
				"departure_airport": "LED",
				"scheduled_arrival": "2016-08-24T15:30:00+00:00",
				"scheduled_departure": "2016-08-24T11:55:00+00:00"
			}
		]
	}
}
```

#### Late Departures ` GET /v1/flights/late-departure/:delay`
Late departures are perfomed with extract function. At first I haven't used hour + minute format, but it gave
me results in decimal, therefore this is a better solution. Arrived status has to be considered and the result
is ordered by delay and flight_id.
```python
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
```
##### Results for `http://localhost:8000/v1/flights/late-departure/250`:
```json
{
	"results": [
		{
			"flight_id": 157571,
			"flight_no": "PG0073",
			"delay": 303
		},
		{
			"flight_id": 186524,
			"flight_no": "PG0040",
			"delay": 284
		},
		{
			"flight_id": 126166,
			"flight_no": "PG0533",
			"delay": 282
		},
		{
			"flight_id": 56731,
			"flight_no": "PG0132",
			"delay": 281
		},
		{
			"flight_id": 102938,
			"flight_no": "PG0531",
			"delay": 281
		},...
    ]
}
```

#### Top Airlines ` GET /v1/top-airlines?limit=:limit`
This query returns the top flights of the database. At first count is selected and 
another table is joined, results must have arrived and are ordered by the count up to
the desired number of records.
```python
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
```
##### Results for `http://localhost:8000/v1/top-airlines?limit=5`:
```json
{
	"results": [
		{
			"flight_no": "PG0222",
			"count": 124392
		},
		{
			"flight_no": "PG0225",
			"count": 121812
		},
		{
			"flight_no": "PG0223",
			"count": 120179
		},
		{
			"flight_no": "PG0226",
			"count": 117843
		},
		{
			"flight_no": "PG0224",
			"count": 117830
		}
	]
}
```

#### Scheduled Flights ` GET /v1/departures?airport=:airport&day=:day`
This query returns all the scheduled departures from the airport at desired day of the week. 
Desired columns are selected at first, airports_data table is joined and extract function
on the day of the week is performed, ordered by time.
```python
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
```
##### Results for `http://localhost:8000/v1/departures?airport=KJA&day=5`:
```json
{
	"results": [
		{
			"flight_id": 91946,
			"flight_no": "PG0689",
			"scheduled_departure": "2017-08-18T04:25:00+00:00"
		},
		{
			"flight_id": 90493,
			"flight_no": "PG0207",
			"scheduled_departure": "2017-08-18T04:35:00+00:00"
		},
		{
			"flight_id": 93191,
			"flight_no": "PG0352",
			"scheduled_departure": "2017-08-18T04:50:00+00:00"
		},
		{
			"flight_id": 92337,
			"flight_no": "PG0021",
			"scheduled_departure": "2017-08-18T05:25:00+00:00"
		},
		{
			"flight_id": 90047,
			"flight_no": "PG0548",
			"scheduled_departure": "2017-08-18T05:40:00+00:00"
		},...
    ]
}
```

#### Flight load ` GET /v1/airlines/:flight_no/load`
Flight load query returns the load of a flight per desired flight_no. It counts the number
of seats in an aircraft and tickets that have been booked for selected flight. Division is
performed in order to get the percentage load. Since the tester accepts 100 as not decimal
number but integer, the case is implemented where 100.00 is replaced by 100. Results are 
grouped by flight_id.
```python
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
    where f.flight_no = \'''' + flight_no + '''\' group by f.flight_id;''')
    exe = local_cursor.fetchall()
    return {
        "results": exe
    }
```
##### Results for `http://localhost:8000/v1/airlines/PG0242/load`:
```json
{
	"results": [
		{
			"id": 187432,
			"aircraft_capacity": 97,
			"load": 81,
			"percentage_load": 83.51
		},
		{
			"id": 187433,
			"aircraft_capacity": 97,
			"load": 86,
			"percentage_load": 88.66
		},
		{
			"id": 187434,
			"aircraft_capacity": 97,
			"load": 96,
			"percentage_load": 98.97
		},
		{
			"id": 187435,
			"aircraft_capacity": 97,
			"load": 89,
			"percentage_load": 91.75
		},
		{
			"id": 187436,
			"aircraft_capacity": 97,
			"load": 79,
			"percentage_load": 81.44
		},
		{
			"id": 187437,
			"aircraft_capacity": 97,
			"load": 89,
			"percentage_load": 91.75
		},
		{
			"id": 187438,
			"aircraft_capacity": 97,
			"load": 91,
			"percentage_load": 93.81
		},...
    ]
}
```

#### Flight Week Load `GET /v1/airlines/:flight_no/load-week`
Flight week load returns the load of each day of the week per desired flight.
Select is not so good as I would be satisfied with, but it returns the
scheduled_departure time and performs the calculations to get percentage_load.
This is then ordered to the fields by day and the average value is found and
appended to the dictionary.
```python
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
```
##### Results for `http://localhost:8000/v1/airlines/PG0242/load-week`:
```json
{
	"result": {
		"flight_no": "PG0242",
		"monday": 81.17,
		"tuesday": 82.66,
		"wednesday": 84.81,
		"thursday": 79.8,
		"friday": 82.25,
		"saturday": 80.25,
		"sunday": 82.88
	}
}
```

### Assignment 1
#### GET /v1/status
Application contains a simple HTTP endpoint `GET /v1/status` which will return a JSON file containing
a current version of the db and the system it is connected to. Prints the first record in the list since
it returns a list of lists.
```python
@router.get("/v1/status")
async def status():
    local_cursor = conn.cursor()
    local_cursor.execute('SELECT version();')
    test = local_cursor.fetchall()
    print(test[0][0])
    return {
        'version': test
    }
```

#### Conclusion
Endpoints have been implemented and are working correctly