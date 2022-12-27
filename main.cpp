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


// Returns float and moves iterator to symbol after the float.
// Does not check if begin points on numeric symbol or dot or '-'
static float pCharToFloat (std::string::iterator& it)
{
    float result = 0;
    bool wasDot = false;
    bool isNegative = (*it == '-');
    if (isNegative)
    {
        ++it;
    }

    for (;; ++it)
    {
        const char c = *it;
        if('0' <= c && c <= '9')
            result = result*10 + (c - '0');
        else 
        {
            if (c == '.')
            {
                wasDot = true;
                ++it;
            }
            break;
        }
    }
    
    if (wasDot)
    {
        float multiplier = 0.1;
        for (;; ++it)
        {
            const char c = *it;
            if ('0' <= c && c <= '9')
            {
                result += (c - '0') * multiplier;
                multiplier /= 10.;
            }
            else 
            {
                break;
            }    
        }
    }

    if (isNegative)
        result *=-1;

    return result;
}

// Returns unsigned int (it means no sign check) and moves iterator to first char after int
// Does not check if begin points on numeric symbol 
static int pCharToInt(std::string::iterator& it)
{
    int result = 0;
    for (; ; ++it)
    {
        const auto c = *it;
        if('0' <= c && c <= '9')
            result = result*10 + (c - '0');
        else
            break;
    }
    return result;
}

// Returns string from @it to ',' and moves iterator to ','
static std::string pCharToString (std::string::iterator& it)
{
    std::string result;
    constexpr size_t maxStrSize = 50;
    result.reserve(maxStrSize);

    for (;; ++it)
    {
        const char c = *it;
        if(c == ',')
            break;

        result.push_back(c);
    }
    return result;
}

struct Row
{
    // Constructor with strings moving
    Row(std::string&& key, float fare_amount, std::string&& pickup_datetime,
        float pickup_longitude, float pickup_latitude, float dropoff_longitude,
        float dropoff_latitude, int passenger_count):
    key(key),
    fare_amount(fare_amount),
    pickup_datetime(pickup_datetime),
    pickup_longitude(pickup_longitude),
    pickup_latitude(pickup_latitude),
    dropoff_longitude(dropoff_longitude),
    dropoff_latitude(dropoff_latitude),
    passenger_count(passenger_count)
    {
    }


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

// Returns std::vector of rows, which are written in string from @begin to @end.
// @size shows recommended size to reserve
static std::vector<Row> getRows(std::string::iterator begin,
                                std::string::iterator end,
                                size_t size)
{
    std::vector<Row> rows;
    rows.reserve(size);
    auto it = begin;

    while (it != end)
    {
        // Read string only once. Interpretate it while reading 
        std::string key = pCharToString(it);
        ++it;
        
        const float fareAmount = pCharToFloat(it);
        ++it;

        std::string datetime = pCharToString(it);
        ++it;

        const float pickupLongitude = pCharToFloat(it);
        ++it;

        const float pickupLatitude = pCharToFloat(it);
        ++it;

        const float dropoffLongitude = pCharToFloat(it);
        ++it;

        const float dropoffLatitude = pCharToFloat(it);
        ++it;

        const int passengerCount = pCharToInt(it);
        it = ++std::find(it, end, '\n');

        // Call Row's constructor once
        rows.emplace_back(
            std::move(key), 
            fareAmount,
            std::move(datetime),
            pickupLongitude,
            pickupLatitude,
            dropoffLongitude,
            dropoffLatitude,
            passengerCount);
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
