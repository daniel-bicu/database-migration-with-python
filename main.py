import psycopg2
import json
from databaseops import  connect_postgresql_db, connect_mongo_db, import_data_to_MongoDb
import config

if __name__ == '__main__':

    conn = connect_postgresql_db(config.connection_object_postgresql)
    print(conn)

    cursor = conn.cursor()

    queryObject = """SELECT Json_build_object('_id', r0.rental_id, 'customer',
              Json_build_object('customer_id', c.customer_id
              , 'first_name', c.first_name
              ,
              'last_name', c.last_name, 'email', c.email, 'active', c.active,
              'address'
       ,
              Json_build_object('address_id', a.address_id, 'address', a.address
              , 'address2', a.address2, 'district', a.district
              , 'postal_code', a.postal_code, 'city',
       Json_build_object('city_id', ci.city_id, 'city', ci.city, 'country',
       Json_build_object
       ('country_id', ct.country_id, 'country', ct.country)), 'phone', a.phone),
              'registred', Json_build_object('create_date', c.create_date,
                           'store',
       Json_build_object('store_id'
       , sr.store_id, 'address',
       Json_build_object('address_id'
       , a2.address_id, 'address', a2.address, 'address2'
       , a2.address2, 'district',
       a2.district, 'postal_code', a2.postal_code, 'city',
       Json_build_object('city_id', ci2.city_id, 'city', ci2.city, 'country',
       Json_build_object
       ('country_id', ct2.country_id, 'country', ct2.country))
       , 'phone', a2.phone)))), 'sale_person',
              Json_build_object('staff_id', sf.staff_id
              , 'first_name', sf.first_name,
              'last_name', sf.last_name, 'email', sf.email
              , 'username', sf.username,
              'address',
              Json_build_object('address_id', a2.address_id
              , 'address', a2.address,
              'address2'
       , a2.address2, 'district', a2.district, 'postal_code', a2.postal_code,
              'city',
       Json_build_object('city_id', ci2.city_id, 'city', ci2.city, 'country',
       Json_build_object
       ('country_id', ct2.country_id, 'country', ct2.country))
              , 'phone', a2.phone),
              'store', Json_build_object('store_id', sr.store_id, 'address',
       Json_build_object('address_id', a2.address_id, 'address', a2.address,
       'address2'
                     , a2.address2, 'district',
       a2.district, 'postal_code', a2.postal_code, 'city',
                                                      Json_build_object(
                                                      'city_id', ci2.city_id,
                                                      'city', ci2.city,
                                                      'country',
                                        Json_build_object
                                        ('country_id', ct2.country_id, 'country'
                                        , ct2.country)),
       'phone', a2.phone))), 'invoice',
       Json_build_object('rental_id', r0.rental_id,
       'rental_date', r0.rental_date,
                                        'return_date',
       r0.return_date), 'movie',
              Json_build_object('film_id', r1.film_id, 'title', r1.title
              , 'description', r1.description, 'release_year', r1.release_year,
              'length', r1."length", 'rating', r1.rating
              , 'special_features', r1.special_features, 'fulltext', r1.fulltext
       ,
              'language',
              Json_build_object('language_id', r2.language_id, 'name', r2."name"
              ),
              'actors', r3.actors, 'rental_duration', r1.rental_duration,
              'rental_rate'
       , r1.rental_rate, 'last_update', r1.last_update), 'payments', r4.payments
       )
FROM   rental r0
       JOIN (SELECT o1.rental_id,
                    f.film_id,
                    f.title,
                    f.description,
                    f.release_year,
                    f."length",
                    f.rating,
                    f.special_features,
                    f.fulltext,
                    f.rental_duration,
                    f.rental_rate,
                    f.last_update
             FROM   rental o1
                    JOIN inventory i
                      ON o1.inventory_id = i.inventory_id
                    JOIN film f
                      ON i.film_id = f.film_id
             ORDER  BY o1.rental_id) r1
         ON r0.rental_id = r1.rental_id
       LEFT JOIN (SELECT o1.rental_id,
                         l.language_id,
                         l."name"
                  FROM   rental o1
                         JOIN inventory i
                           ON o1.inventory_id = i.inventory_id
                         JOIN film f
                           ON i.film_id = f.film_id
                         JOIN language l
                           ON f.language_id = l.language_id
                  ORDER  BY o1.rental_id) r2
              ON r0.rental_id = r2.rental_id
       LEFT JOIN (SELECT act.rental_id,
                         Array_agg(actors) AS actors
                  FROM   (SELECT o1.rental_id,
Json_build_object('actor_id', ac.actor_id, 'first_name', ac.first_name, 'last_name', ac.last_name) AS actors
 FROM   rental o1
        JOIN inventory i
          ON o1.inventory_id = i.inventory_id
        JOIN film f
          ON i.film_id = f.film_id
        JOIN film_actor fa
          ON f.film_id = fa.film_id
        LEFT JOIN actor ac
               ON fa.actor_id = ac.actor_id
 ORDER  BY o1.rental_id) act
 GROUP  BY act.rental_id) r3
       ON r0.rental_id = r3.rental_id
LEFT JOIN (SELECT pay.rental_id,
                  Array_agg(pay.payments) AS payments
           FROM   (SELECT
       o1.rental_id,
          Json_build_object('payment_id',
pay.payment_id, 'amount', pay.amount, 'payment_date', pay.payment_date)
        AS
payments
         FROM   rental o1
                JOIN payment pay
                  ON o1.rental_id = pay.rental_id
         ORDER  BY o1.rental_id) pay
 GROUP  BY pay.rental_id) r4
ON r0.rental_id = r4.rental_id
LEFT JOIN customer c
       ON r0.customer_id = c.customer_id
LEFT JOIN address a
       ON c.address_id = a.address_id
LEFT JOIN city ci
       ON a.city_id = ci.city_id
LEFT JOIN country ct
       ON ci.country_id = ct.country_id
LEFT JOIN store sr
       ON c.store_id = sr.store_id
LEFT JOIN staff sf
       ON sr.store_id = sf.store_id
LEFT JOIN address a2
       ON sr.address_id = a2.address_id
LEFT JOIN city ci2
       ON a2.city_id = ci2.city_id
LEFT JOIN country ct2
       ON ci2.country_id = ct2.country_id """

    cursor.execute(queryObject)
    rows = cursor.fetchall()
    rows = [row[0] for row in rows]

    datafile = r"rental_data.json"

    with open(datafile, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=4)

    import_data_to_MongoDb(connect_mongo_db(config.connection_string_mongodb), datafile)
