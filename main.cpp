#include <algorithm>
#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <thread>

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

class Table
{
public:
    Table(std::vector<std::vector<Row>> &&rows) : rows(std::move(rows)) {}

    uint64_t rows_count() const
    {
        uint64_t result = 0;
        for (const auto &vector : rows)
            result += vector.size();

        return result;
    }

    Row get_row(uint64_t idx) const
    {
        size_t i = 0;
        for (;; ++i)
        {
            const size_t vectorSize = rows[i].size();
            if (idx >= vectorSize)
                idx -= vectorSize;
            else
                break;
        }
        return rows[i][idx];
    }

private:
    std::vector<std::vector<Row>> rows;
};

// Returns std::vector of rows, which are written in string from start to begin
// size shows recommended size to reserve
static std::vector<Row> getRows(std::string::iterator begin, std::string::iterator end,
                         size_t size)
{
    std::vector<Row> rows;
    rows.reserve(size);
    auto it = begin;

    while (it != end)
    {

        
        Row row;

        {
            auto keyEnd = std::find(it, end, ',');
            row.key.assign(&*it, std::distance(it, keyEnd));
            it = ++keyEnd;
        }

        row.fare_amount = atof(&*it);
        it = ++std::find(it, end, ',');

        {
            auto datetimeEnd = std::find(it, end, ',');
            row.pickup_datetime.assign(&*it, std::distance(it, datetimeEnd));
            it = ++datetimeEnd;
        }

        row.pickup_longitude = atof(&*it);
        it = ++std::find(it, end, ',');

        row.pickup_latitude = atof(&*it);
        it = ++std::find(it, end, ',');

        row.dropoff_longitude = atof(&*it);
        it = ++std::find(it, end, ',');

        row.dropoff_latitude = atof(&*it);
        it = ++std::find(it, end, ',');

        row.passenger_count = std::atoi(&*it);

        it = ++std::find(it, end, '\n');

        rows.push_back(row);
    }
    return rows;
}

Table read_csv(const char *filename)
{

    const size_t smallStringSize = 70;

    // Read file like to a single string to make parallel parsing possible
    std::fstream file(filename);
    if(!file.is_open())
        throw std::runtime_error("File does not exist");

    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    std::string fileString(size, ' ');
    file.seekg(0);
    file.read(&fileString[0], size);

    // skip header
    auto it = ++std::find(fileString.begin(), fileString.end(), '\n');
    size -= std::distance(fileString.begin(), it);

    const auto threadsNb = std::thread::hardware_concurrency();
    std::vector<std::vector<Row>> rows;
    rows.resize(threadsNb);

    std::vector<std::thread> threads;

    const size_t substringSize = size / threadsNb;

    for (size_t i = 0; i < threadsNb; ++i)
    {
        auto preliminaryBegin = fileString.begin() + i * substringSize;
        auto begin = i == 0 ? it : ++std::find(preliminaryBegin, fileString.end(), '\n');
        auto end = i == threadsNb -1  ?
            fileString.end() :
            ++std::find(fileString.begin() + (i + 1) * substringSize, fileString.end(), '\n'); 

        // Parses part of string and form rows[i], 
        // where i is a number of thread and number of fileString piece 
        auto parsePart = [&rows, begin, end, smallStringSize, i](){       
        rows[i] = getRows(begin, end, std::distance(begin, end) / smallStringSize);
        };

        threads.push_back(std::thread (parsePart));
    }

    for (auto& t : threads)
    {
          t.join();
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
