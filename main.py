import asyncio
import json
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer

class Forbrug:
    # https://www.bolius.dk/saa-meget-el-vand-og-varme-bruger-en-gennemsnitsfamilie-279
    M2 = 100
    kWhPrVoksen = 0.1216895
    kWhPrBarn = 0.1216895
    kWhElBil = 0.456621
    m3VandPrVoksen = 0.00570776255
    m3VandPrBarn = 0.00251141552

    @staticmethod
    async def forbrug_main():
        json_file_path = 'houses.json'
        with open(json_file_path) as json_file:
            houses = json.load(json_file)

        tasks = []

        for house in houses:
            tasks.append(Forbrug.generate_power(house))
            print(f"Power generation for house {house['id']} has started")
            tasks.append(Forbrug.generate_water(house))
            print(f"Water generation for house {house['id']} has started")

        await asyncio.gather(*tasks)

    @staticmethod
    async def generate_water(house):
        date_time = datetime(2020, 1, 1)

        producer_config = {
            'bootstrap.servers': '172.16.250.13:9092,172.16.250.14:9092,172.16.250.15:9092,172.16.250.16:9092,'
                                '172.16.250.17:9092,172.16.250.18:9092,172.16.250.19:9092,172.20.250.16:9092,'
                                '172.16.250.21:9092'
        }

        p = Producer(producer_config)

        while date_time < datetime(2022, 1, 1):
            total_m3 = (house['no_adults'] * Forbrug.m3VandPrVoksen) + (house['no_children'] * Forbrug.m3VandPrBarn)
            total_m3 = (total_m3 / house['house_size_m2'] * 100)

            percentage_change = random.uniform(0.01, 1)
            change = total_m3 * percentage_change
            new_total = total_m3 + change
            month = Forbrug.get_month_multiplier(date_time.month)
            hour = Forbrug.get_hour_multiplier(date_time.hour)
            new_total = new_total * month
            await Forbrug.produce("Water", "m3", p, house['id'], new_total * hour, int(date_time.timestamp()))
            date_time += timedelta(hours=1)

        p.flush()

    @staticmethod
    async def generate_power(house):
        date_time = datetime(2020, 1, 1)

        producer_config = {
            'bootstrap.servers': '172.16.250.13:9092,172.16.250.14:9092,172.16.250.15:9092,172.16.250.16:9092,'
                                '172.16.250.17:9092,172.16.250.18:9092,172.16.250.19:9092,172.20.250.16:9092,'
                                '172.16.250.21:9092'
        }

        p = Producer(producer_config)

        while date_time < datetime(2022, 1, 1):
            total_kWh = (house['no_adults'] * Forbrug.kWhPrVoksen) + (house['no_children'] * Forbrug.kWhPrBarn) + (
                    house['no_electric_cars'] * Forbrug.kWhElBil)
            total_kWh = (total_kWh / house['house_size_m2'] * 100)

            percentage_change = random.uniform(0.01, 1)
            change = total_kWh * percentage_change
            new_total = total_kWh + change
            month = Forbrug.get_month_multiplier(date_time.month)
            hour = Forbrug.get_hour_multiplier(date_time.hour)
            new_total = new_total * month
            await Forbrug.produce("Power", "kwh", p, house['id'], new_total * hour, int(date_time.timestamp()))
            date_time += timedelta(hours=1)

        p.flush()

    @staticmethod
    async def produce(topic, type, producer, house_id, kwH, dt):
        try:
            value = {
                "house_id": house_id,
                "timestamp": dt,
                type: round(kwH, 20)
            }
            producer.produce(topic, key=str(house_id), value=json.dumps(value))
        except Exception as ex:
            print(f"Delivery failed: {ex}")

    @staticmethod
    def get_hour_multiplier(hour):
        hour_dict = {
            0: 0.60,
            1: 0.50,
            2: 0.50, 
            3: 0.50, 
            4: 0.50, 
            5: 0.50,
            6: 1.00, 
            7: 1.00, 
            8: 1.10, 
            9: 1.25, 
            10: 1.50, 
            11: 1.50,
            12: 1.50, 
            13: 1.15, 
            14: 1.00, 
            15: 1.00, 
            16: 1.00, 
            17: 1.50, 
            18: 1.50, 
            19: 1.50, 
            20: 1.00, 
            21: 0.90, 
            22: 0.80, 
            23: 0.70
        }
        return hour_dict.get(hour, 0)

    @staticmethod
    def get_month_multiplier(month):
        month_dict = {1: 1, 2: 0.95, 3: 0.90, 4: 0.85, 5: 0.80, 6: 0.75, 7: 0.75, 8: 0.80, 9: 0.85, 10: 0.90, 11: 0.95, 12: 1}
        return month_dict.get(month, 0)

if __name__ == "__main__":
    asyncio.run(Forbrug.forbrug_main())
