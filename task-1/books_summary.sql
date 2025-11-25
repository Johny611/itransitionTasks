DROP TABLE IF EXISTS books_summary;

CREATE TABLE books_summary AS
SELECT
    year AS publication_year,
    COUNT(*) AS book_count,
    ROUND(
        AVG(
            CASE 
                WHEN price_currency = 'EUR' THEN price_value * 1.2
                ELSE price_value
            END
        ), 2
    ) AS average_price
FROM books_raw
GROUP BY year
ORDER BY year;


SELECT COUNT(*) FROM books_raw;
SELECT COUNT(*) FROM books_summary;
SELECT * FROM books_summary ORDER BY publication_year




