-- Revenue Analysis
-- Revenue Analysis - Quarter
SELECT 
    CONCAT(EXTRACT(YEAR FROM DATE(p.date)), ' Q', EXTRACT(QUARTER FROM DATE(p.date))) AS quarter, 
    ROUND(AVG(b.amount)::NUMERIC, 2) AS avg_amount,
    ROUND(SUM(b.amount)::NUMERIC, 2) AS total_amount
FROM payment p
JOIN booking b ON p.booking_id = b.booking_id
JOIN (
    SELECT DISTINCT card_num, customer_id
    FROM credit_card
  ) cc ON p.card_num = cc.card_num
JOIN customer c ON cc.customer_id = c.customer_id
WHERE DATE(p.date) < date_trunc('quarter', current_date)::date
GROUP BY quarter
HAVING EXTRACT(YEAR FROM MAX(p.date)) < EXTRACT(YEAR FROM current_date)::int OR 
       (EXTRACT(YEAR FROM MAX(p.date)) = EXTRACT(YEAR FROM current_date)::int AND 
        EXTRACT(QUARTER FROM MAX(p.date)) < EXTRACT(QUARTER FROM current_date)::int)
ORDER BY quarter;

-- Revenue Analysis - Year
SELECT 
    CONCAT(EXTRACT(YEAR FROM DATE(p.date))) AS year, 
    ROUND(AVG(b.amount)::NUMERIC, 2) AS avg_amount,
    ROUND(SUM(b.amount)::NUMERIC, 2) AS total_amount
FROM payment p
JOIN booking b ON p.booking_id = b.booking_id
JOIN (
    SELECT DISTINCT card_num, customer_id
    FROM credit_card
  ) cc ON p.card_num = cc.card_num
JOIN customer c ON cc.customer_id = c.customer_id
WHERE DATE(p.date) < date_trunc('year', current_date)::date
GROUP BY year
HAVING EXTRACT(YEAR FROM MAX(p.date)) < EXTRACT(YEAR FROM current_date)::int
ORDER BY year;

-- Total Revenue YoY - Break Down
SELECT 
    EXTRACT(YEAR FROM p.date) AS year,
    COALESCE(SUM(CASE WHEN b.leaving_flight_r_id IS NOT NULL THEN b.amount END), 0) AS flight_revenue,
    COALESCE(SUM(CASE WHEN b.car_r_id IS NOT NULL THEN b.amount END), 0) AS car_revenue,
    COALESCE(SUM(CASE WHEN b.hotel_r_id IS NOT NULL THEN b.amount END), 0) AS hotel_revenue
FROM payment p
JOIN booking b ON p.booking_id = b.booking_id
GROUP BY year
ORDER BY year;

-- Total Revenue - Break Down By State
SELECT 
    EXTRACT(YEAR FROM p.date) AS year,
    c.state,
    COALESCE(SUM(b.amount), 0) AS total_revenue
FROM payment p
JOIN booking b ON p.booking_id = b.booking_id
JOIN credit_card cc ON p.card_num = cc.card_num
JOIN customer c ON cc.customer_id = c.customer_id
WHERE EXTRACT(YEAR FROM p.date) = DATE_PART('year', CURRENT_DATE)-1
GROUP BY year, c.state
ORDER BY total_revenue DESC, c.state;

-- Spending Analysis - Top Customers
SELECT 
  c.first_name || ' ' || c.last_name AS customer_name,
  SUM(b.amount) AS total_spent
FROM booking b
JOIN payment p ON b.booking_id = p.booking_id
JOIN (
	SELECT DISTINCT card_num, customer_id
    FROM credit_card
  ) cc ON p.card_num = cc.card_num
JOIN customer c ON cc.customer_id = c.customer_id
GROUP BY customer_name
ORDER BY total_spent DESC
LIMIT 5;

-- Spending Segment Analysis
WITH customer_total_spent AS (
  SELECT 
    c.first_name || ' ' || c.last_name AS customer_name,
    SUM(b.amount) AS total_spent,
    RANK() OVER (ORDER BY SUM(b.amount) DESC) AS rank,
    COUNT(*) OVER () AS total_customers
  FROM booking b
  JOIN payment p ON b.booking_id = p.booking_id
  JOIN (
    SELECT DISTINCT card_num, customer_id
    FROM credit_card
  ) cc ON p.card_num = cc.card_num
  JOIN customer c ON cc.customer_id = c.customer_id
  WHERE p.date >= '2022-01-01' AND p.date < '2023-01-01'
  GROUP BY customer_name
)
SELECT 
  CEIL(10.0 * rank / total_customers) AS bucket,
  ROUND(AVG(total_spent)::NUMERIC, 2) AS avg_spent
FROM customer_total_spent
GROUP BY bucket
ORDER BY bucket;


-- Customer Analysis
-- Customers' Geographical distribution
SELECT state, count(customer_id)
FROM customer
GROUP BY state;

-- Booking Analysis
SELECT 
    COALESCE(all_reservations.month, flight_reservations.month, car_reservations.month, hotel_reservations.month) AS month, 
    COALESCE(all_reservations.reservations, 0) AS all_reservations, 
    COALESCE(flight_reservations.flight_reservations, 0) AS flight_reservations, 
    COALESCE(car_reservations.car_reservations, 0) AS car_reservations, 
    COALESCE(hotel_reservations.hotel_reservations, 0) AS hotel_reservations
FROM (
    SELECT COUNT(*) AS reservations, EXTRACT(MONTH FROM booking_date) AS month
    FROM (
        SELECT booking_date FROM hotel_reservation
        UNION ALL
        SELECT booking_date FROM flight_reservation
        UNION ALL
        SELECT DATE(booking_date) AS booking_date FROM car_reservation
    ) AS reservations_combined
    GROUP BY month
) AS all_reservations
LEFT JOIN (
    SELECT COUNT(*) AS flight_reservations, EXTRACT(MONTH FROM booking_date) AS month
    FROM flight_reservation
    GROUP BY month
) AS flight_reservations ON all_reservations.month = flight_reservations.month
LEFT JOIN (
    SELECT COUNT(*) AS car_reservations, EXTRACT(MONTH FROM DATE(booking_date)) AS month
    FROM car_reservation
    GROUP BY month
) AS car_reservations ON all_reservations.month = car_reservations.month
LEFT JOIN (
    SELECT COUNT(*) AS hotel_reservations, EXTRACT(MONTH FROM booking_date) AS month
    FROM hotel_reservation
    GROUP BY month

-- Booking Analysis - Hotel
SELECT COUNT(*) AS hotel_reservations, EXTRACT(MONTH FROM booking_date) AS month
FROM hotel_reservation
GROUP BY month
ORDER BY hotel_reservations DESC;

-- Booking Analysis - Flight
SELECT COUNT(*) AS flight_reservations, EXTRACT(MONTH FROM booking_date) AS month
FROM flight_reservation
GROUP BY month
ORDER BY flight_reservations DESC;

-- Booking Analysis - Car Rental
SELECT COUNT(*) AS car_reservations, EXTRACT(MONTH FROM DATE(booking_date)) AS month
FROM car_reservation
GROUP BY month
ORDER BY car_reservations DESC;


-- Hotel Analysis
-- Stay Length and Total Charge
SELECT h.name, h.stars, 
       ROUND(AVG(check_out_date - check_in_date)::NUMERIC, 2) AS avg_stay_length, 
       ROUND(AVG(total_charge)::NUMERIC, 2) AS avg_total_charge
FROM hotel_reservation hrz
JOIN room r ON hrz.room_id = r.room_id 
JOIN hotel h ON r.hotel_id = h.hotel_id 
GROUP BY h.name, h.stars
ORDER BY avg_total_charge DESC
LIMIT 10;

-- Satisfaction Analysis - Hotel
SELECT h.name, h.stars, ROUND(AVG(hr.rating)::NUMERIC, 2) as avg_rating
FROM hotel_review hr
JOIN hotel_reservation hrz ON hr.reservation_id = hrz.reservation_id
JOIN room r ON hrz.room_id = r.room_id
JOIN hotel h ON r.hotel_id = h.hotel_id
GROUP BY h.hotel_id
ORDER BY avg_rating DESC
LIMIT 5;

-- Hotel Occupancy Rate - Last Year
WITH cte AS (
    SELECT 
        DATE_PART('year', check_out_date) AS years, 
        h.name, 
        h.city, 
        h.state,
        ROUND(SUM(r.check_out_date - r.check_in_date)::DECIMAL / (h.num_rooms * 20)*100, 2) AS occupancy_rate
    FROM hotel_reservation r
	JOIN room rm ON rm.room_id = r.room_id
    LEFT JOIN hotel h ON h.hotel_id = rm.hotel_id
    GROUP BY years, h.name, h.city, h.state, h.num_rooms
    ORDER BY years, occupancy_rate DESC
)
SELECT *
FROM cte
WHERE DATE_PART('year', CURRENT_DATE)-1 = years AND occupancy_rate > 10
ORDER BY years, occupancy_rate DESC
LIMIT 10;

-- Hotel Occupancy Rate - The Year Before Last
WITH cte AS (
    SELECT 
        DATE_PART('year', check_out_date) AS years, 
        h.name, 
        h.city, 
        h.state,
        ROUND(SUM(r.check_out_date - r.check_in_date)::DECIMAL / (h.num_rooms * 20)*100, 2) AS occupancy_rate
    FROM hotel_reservation r
	JOIN room rm ON rm.room_id = r.room_id
    LEFT JOIN hotel h ON h.hotel_id = rm.hotel_id
    GROUP BY years, h.name, h.city, h.state, h.num_rooms
    ORDER BY years, occupancy_rate DESC
)
SELECT *
FROM cte
WHERE DATE_PART('year', CURRENT_DATE)-2 = years AND occupancy_rate > 10
ORDER BY years, occupancy_rate DESC
LIMIT 10;

-- Hotel Occupancy Rate - Last 3 Year
WITH cte AS (
    SELECT 
        DATE_PART('year', check_out_date) AS years, 
        h.name, 
        h.city, 
        h.state,
        ROUND(SUM(r.check_out_date - r.check_in_date)::DECIMAL / (h.num_rooms * 20)*100, 2) AS occupancy_rate
    FROM hotel_reservation r
	JOIN room rm ON rm.room_id = r.room_id
    LEFT JOIN hotel h ON h.hotel_id = rm.hotel_id
    GROUP BY years, h.name, h.city, h.state, h.num_rooms
    ORDER BY years, occupancy_rate DESC
)
SELECT *
FROM cte
WHERE DATE_PART('year', CURRENT_DATE)-3 = years AND occupancy_rate > 10
ORDER BY years, occupancy_rate DESC
LIMIT 10;


-- Flight Analysis
SELECT EXTRACT(MONTH FROM departure_date) AS departure_month, COUNT(*) AS total_flights
FROM flight
GROUP BY departure_month
ORDER BY total_flights DESC
LIMIT 3;

-- Flight Peak Months
SELECT EXTRACT(MONTH FROM departure_date) AS departure_month, COUNT(*) AS total_flights
FROM flight
GROUP BY departure_month
ORDER BY total_flights DESC;

-- Busiest Airports - Top 5
WITH city AS (
  SELECT f.flight_id, f.route_id, ft.landing_airport
  FROM flight_reservation fr
  JOIN flight f ON fr.reservation_id = f.reservation_id
  JOIN flight_route ft ON ft.route_id = f.route_id
)
SELECT landing_airport, COUNT(landing_airport) AS total_flights
FROM city
GROUP BY landing_airport
ORDER BY total_flights DESC
LIMIT 5;

-- Least Busy Airports - Top 5
WITH city AS (
  SELECT f.flight_id, f.route_id, ft.landing_airport
  FROM flight_reservation fr
  JOIN flight f ON fr.reservation_id = f.reservation_id
  JOIN flight_route ft ON ft.route_id = f.route_id
)
SELECT landing_airport, COUNT(landing_airport) AS total_flights
FROM city
GROUP BY landing_airport
ORDER BY total_flights ASC
LIMIT 5;

-- Travel Size
WITH travel_size AS (
  SELECT f.reservation_id, SUM(t.num_tickets) AS travel_size
  FROM flight_reservation f
  JOIN flight fl ON f.reservation_id = fl.reservation_id
  JOIN (
    SELECT flight_id, COUNT(ticket_id) AS num_tickets
    FROM ticket
    GROUP BY flight_id
  ) t ON fl.flight_id = t.flight_id
  GROUP BY f.reservation_id
)
SELECT travel_size, COUNT(*) AS num_reservations
FROM travel_size
GROUP BY travel_size
ORDER BY num_reservations DESC;


-- Car Analysis
Satisfaction Analysis - Car
SELECT c.brand, ROUND(AVG(cr.car_rating)::NUMERIC, 2) as avg_rating
FROM car_review cr
JOIN car_reservation crz ON cr.reservation_id = crz.reservation_id
JOIN car c ON crz.car_id = c.car_id
GROUP BY c.brand
ORDER BY avg_rating DESC
LIMIT 5;
