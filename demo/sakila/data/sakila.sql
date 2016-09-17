-- DIMENSIONS
SELECT * FROM (
SELECT 'TOTACTOR', 'TOTAL ACTORS', ''
UNION
SELECT CONCAT('A', actor_id), CONCAT(first_name, ' ', last_name), 'TOTACTOR' FROM actor) t
INTO OUTFILE 'K:/Workspaces/Python/cubely/cubely/demo/sakila/data/actors.txt';


SELECT * FROM (
SELECT 'TOTGEOG', 'TOTAL GEOGRAPHY', ''
UNION
SELECT CONCAT('GO', country_id), country, 'TOTGEOG' FROM country
UNION
SELECT CONCAT('GI', city_id), city, CONCAT('GO', country_id) FROM city
UNION
SELECT CONCAT('GA', address_id), CONCAT(address), CONCAT('GI', city_id) FROM address
UNION
SELECT CONCAT('GC', customer_id), CONCAT(first_name, ' ', last_name), CONCAT('GA', address_id) FROM customer) t
INTO OUTFILE 'K:/Workspaces/Python/cubely/cubely/demo/sakila/data/geography.txt';

SELECT * FROM (
SELECT 'TOTPROD', 'TOTAL PRODUCTS', ''
UNION
SELECT CONCAT('FC', category_id), NAME, 'TOTPROD' FROM category
UNION
SELECT CONCAT('F', film.film_id), title, CONCAT('FC', film_category.category_id) FROM film, film_category WHERE film.film_id = film_category.film_id
) t
INTO OUTFILE 'K:/Workspaces/Python/cubely/cubely/demo/sakila/data/prods.txt';

SELECT * FROM (
SELECT DISTINCT DATE_FORMAT(payment_date, 'Y%Y'), DATE_FORMAT(payment_date, 'Year %Y'), ''   FROM payment
UNION
SELECT DISTINCT DATE_FORMAT(payment_date, 'M%m%Y'), DATE_FORMAT(payment_date, '%M %Y'), DATE_FORMAT(payment_date, 'Y%Y')  FROM payment
) t
INTO OUTFILE 'K:/Workspaces/Python/cubely/cubely/demo/sakila/data/time.txt';

-- DATA FILES
SELECT CONCAT('F', film_id) prod, COUNT(*) inventories FROM inventory GROUP BY film_id
INTO OUTFILE 'K:/Workspaces/Python/cubely/cubely/demo/sakila/data/inventory.txt';

SELECT CONCAT('F', i.film_id) prod, CONCAT('GC', r.customer_id) geog, DATE_FORMAT(r.rental_date, 'M%m%Y') time, COUNT(*) rentals
FROM inventory i, rental r
WHERE r.inventory_id = i.inventory_id
GROUP BY CONCAT('F', i.film_id) , CONCAT('GC', r.customer_id), DATE_FORMAT(r.rental_date, 'M%m%Y')
INTO OUTFILE 'K:/Workspaces/Python/cubely/cubely/demo/sakila/data/rentals.txt';

SELECT CONCAT('F', i.film_id) prod, CONCAT('GC', r.customer_id) geog, DATE_FORMAT(r.rental_date, 'M%m%Y') time, SUM(p.amount) sales
FROM inventory i, rental r, payment p
WHERE r.inventory_id = i.inventory_id
AND p.rental_id = r.rental_id
GROUP BY CONCAT('F', i.film_id) , CONCAT('GC', r.customer_id), DATE_FORMAT(r.rental_date, 'M%m%Y')
INTO OUTFILE 'K:/Workspaces/Python/cubely/cubely/demo/sakila/data/sales.txt';

