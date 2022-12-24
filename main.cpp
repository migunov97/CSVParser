#include <iostream>
#include <vector>
#include <string>
#include <fstream>

// Задача:
// Нужно оптимизировать парсер csv любыми методами.
// Некоторые из возможных оптимизаций:
// - Распараллелить парсинг с помощью TBB
// - Минимизировать колличество аллокаций

// Менять можно все что угодно, вплоть до структуры таблицы, главное, чтобы методы rows_count и get_row работали.

// ссылка на датасет https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/data?select=train.csv
// (надо зарегистрироваться на kaggle чтобы можно было скачать)

// Вопросы можно задавать сюда https://t.me/fexolm

struct Row
{
    std::string key;
    float fare_amount;
    std::string pickup_datetime;
    float pickup_longitude;
    float pickup_latitude;
    float dropoff_longitude;
    float dropoff_latitude;
    int passenger_count;
};

struct TableImpl
{
    std::vector<Row> rows;
};

class Table
{
public:
    Table(std::vector<Row> &&rows) : rows(std::move(rows)) {}

    uint64_t rows_count() const
    {
        return rows.size();
    }

    Row get_row(uint64_t idx) const
    {
        return rows[idx];
    }

private:
    std::vector<Row> rows;
};

float stof1(const std::string &s) {
    if(s.size() == 0) {
        return 0.f;
    }
    return std::stof(s);
}

Table read_csv(const char *filename)
{
    std::vector<Row> rows;

    std::fstream file(filename);

    if(!file.is_open()) {
        throw std::runtime_error("File does not exist");
    }

    {
        // skip header
        std::string key, fare_amount, pickup_datetime, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, passenger_count;
        std::getline(file, key, ',');
        std::getline(file, fare_amount, ',');
        std::getline(file, pickup_datetime, ',');
        std::getline(file, pickup_longitude, ',');
        std::getline(file, pickup_latitude, ',');
        std::getline(file, dropoff_longitude, ',');
        std::getline(file, dropoff_latitude, ',');
        std::getline(file, passenger_count);
    }

    while (!file.eof())
    {
        Row row;
        std::string key, fare_amount, pickup_datetime, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, passenger_count;
        std::getline(file, key, ',');
        std::getline(file, fare_amount, ',');
        std::getline(file, pickup_datetime, ',');
        std::getline(file, pickup_longitude, ',');
        std::getline(file, pickup_latitude, ',');
        std::getline(file, dropoff_longitude, ',');
        std::getline(file, dropoff_latitude, ',');
        std::getline(file, passenger_count);

        row.key = key;
        row.fare_amount = stof1(fare_amount);
        row.pickup_datetime = pickup_datetime;
        row.pickup_longitude = stof1(pickup_longitude);
        row.pickup_latitude = stof1(pickup_latitude);
        row.dropoff_longitude = stof1(dropoff_longitude);
        row.dropoff_latitude = stof1(dropoff_latitude);
        row.passenger_count = std::stoi(passenger_count);

        rows.push_back(row);
    }

    return Table(std::move(rows));
}

int main()
{
    try {
        Table table = read_csv("train.csv");
    } catch(std::exception &e) {
        std::cerr << e.what() << std::endl;
    }
}
