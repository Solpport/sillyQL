// Project Identifier: C0F4DFE8B340D81183C208F70F9D2D797908754D

#include <functional>
#include <iostream>
#include <stdexcept>
#include <iostream>
#include <unordered_map>
#include <map>
#include <set>
#include <getopt.h>
#include <limits>
#include <variant>

class tableOptions
{
public:
	tableOptions(int argc, char *argv[])
	{
		const char *short_opts = "hq";
		const option long_opts[] = {
			{"help", no_argument, nullptr, 'h'},
			{"quiet", no_argument, nullptr, 'q'},
			{nullptr, 0, nullptr, 0}};

		int opt;
		while ((opt = getopt_long(argc, argv, short_opts, long_opts, nullptr)) != -1)
		{
			switch (opt)
			{
			case 'h':
				printHelp();
				exit(0);
				break;
			case 'q':
				quiet = true;
				break;
			default:
				break;
			}
		}
	}

	// getter functions
	bool isQuiet() const
	{
		return quiet;
	}

private:
	// print help message
	void printHelp() const
	{
		std::cout << "Usage: silly [options]\n"
				  << "-h, --help\t\tPrint this help message and exit.\n"
				  << "-q, --quiet\t\tRun in quiet mode.\n";
	}

	// quiet mode
	bool quiet = false;
};

template <typename T>
int compare_3way(T a, T b)
{
	if (a < b)
		return -1;
	if (a > b)
		return 1;
	return 0;
}

template <>
int compare_3way(const std::string &a, const std::string &b) { return a.compare(b); }

template <typename T>
T from_string(const std::string &str);

template <>
bool from_string(const std::string &str) { return str == "true"; }

template <>
std::string from_string(const std::string &str) { return str; }

template <>
int from_string(const std::string &str) { return std::stoi(str); }

template <>
double from_string(const std::string &str) { return std::stod(str); }

enum class compare_t : int
{
	greater = 1,
	less = -1,
	equals = 0,
};

enum class index_type : bool
{
	bst,
	hash,
};

class index_base;

class column_base
{
public:
	column_base(std::string_view _name) : name{_name} {}
	std::string name;

	virtual void print(std::size_t row) const = 0;

	virtual int compare(std::size_t row, const void *value) const = 0;
	// rows must be sorted
	virtual void delete_rows(const std::vector<std::size_t> &rows) = 0;

	virtual index_base *generate_index(index_type type) = 0;

	// won't be dynamically allocated
	virtual void *buff_from_str(const std::string &str) const = 0;
	// won't be dynamically allocated
	virtual void *buff_from_row(std::size_t row) const = 0;

	virtual void push_back(const void *value) = 0;
	virtual std::size_t size() const = 0;
	virtual ~column_base() = default;
};

template <typename T>
class column : public column_base
{
public:
	column(std::string_view name) : column_base(name) {}

	void print(std::size_t row) const override { std::cout << data[row]; }

	int compare(std::size_t row, const void *value) const override { return compare_3way(data[row], *reinterpret_cast<const T *>(value)); }
	void delete_rows(const std::vector<std::size_t> &rows) override
	{
		if (rows.empty())
			return;
		
		std::size_t cur_index = 0;
		auto it = rows.begin();
		for (std::size_t i = 0; i < data.size(); ++i)
		{
			if (it == rows.end())
				data[cur_index++] = data[i];
			else
			{
				if (*it != i)
					data[cur_index++] = data[i];
				else
					++it;
			}
		}

		data.erase(data.begin() + cur_index, data.end());
	}

	index_base *generate_index(index_type type) override;

	void *buff_from_str(const std::string &str) const override
	{
		buffer = from_string<T>(str);
		return &buffer;
	}
	void *buff_from_row(std::size_t row) const override
	{
		buffer = data[row];
		return &buffer;
	}

	void push_back(const void *value) override { data.push_back(*reinterpret_cast<const T *>(value)); }
	std::size_t size() const override { return data.size(); }

	std::vector<T> data;
	mutable T buffer;
};

class index_base
{
public:
	index_base(column_base *owner) : ref{owner} {}

	virtual void update(bool do_update) = 0;
	virtual void push_back(const void *value) = 0;
	virtual std::size_t distinct() const = 0;

	virtual void do_on_matching_rows(const void *value, compare_t comp, std::function<void(std::size_t)> func) = 0;
	// don't use with multithreading, may return static data
	// for use with delete_rows, returns sorted indices
	virtual const std::vector<std::size_t> &get_matching_rows(const void *value, compare_t comp) = 0;

	virtual ~index_base() = default;

	column_base *ref;
};

template <typename T>
class hash_index : public index_base
{
public:
	hash_index(column_base *owner) : index_base(owner), needs_to_update{false}
	{
		update_map();
	}

	void update(bool do_update) override
	{
		needs_to_update = do_update;
	}

	void push_back(const void *value) override
	{
		if (needs_to_update)
		{
			update_map();
			needs_to_update = false;
		}

		auto interpreted_ref = dynamic_cast<column<T> *>(ref);

		auto &data = *reinterpret_cast<const T *>(value);
		map[data].push_back(interpreted_ref->size());
		interpreted_ref->data.push_back(data);
	}

	std::size_t distinct() const override { return map.size(); }

	void do_on_matching_rows(const void *value, compare_t comp, std::function<void(std::size_t)> func) override
	{
		if (comp == compare_t::equals)
		{
			if (needs_to_update)
			{
				update_map();
				needs_to_update = false;
			}

			auto it = map.find(*reinterpret_cast<const T *>(value));
			if (it != map.end())
				for (auto i : it->second)
					func(i);
		}
		else
		{
			for (std::size_t i = 0; i < ref->size(); ++i)
				if (ref->compare(i, value) == static_cast<int>(comp))
					func(i);
		}
	}

	const std::vector<std::size_t> &get_matching_rows(const void *value, compare_t comp) override
	{
		static std::vector<std::size_t> buffer;
		buffer.clear();
		if (comp == compare_t::equals)
		{
			if (needs_to_update)
			{
				update_map();
				needs_to_update = false;
			}

			auto it = map.find(*reinterpret_cast<const T *>(value));
			if (it != map.end())
				return it->second;
			else
				return buffer;
		}
		else
		{
			buffer.reserve(ref->size());
			for (std::size_t i = 0; i < ref->size(); ++i)
				if (ref->compare(i, value) == static_cast<int>(comp))
					buffer.push_back(i);
			
			return buffer;
		}
	}

private:
	std::unordered_map<T, std::vector<std::size_t>> map;
	bool needs_to_update;

	void update_map()
	{
		auto interpreted_ref = dynamic_cast<column<T> *>(ref);
		map.clear();
		map.reserve(interpreted_ref->data.size());
		for (std::size_t i = 0; i < interpreted_ref->data.size(); ++i)
			map[interpreted_ref->data[i]].push_back(i);
	}
};

template <typename T>
class bst_index : public index_base
{
public:
	bst_index(column_base *owner) : index_base(owner), needs_to_update{false}
	{
		update_map();
	}

	void update(bool do_update) override
	{
		needs_to_update = do_update;
	}

	void push_back(const void *value) override
	{
		if (needs_to_update)
		{
			update_map();
			needs_to_update = false;
		}

		auto interpreted_ref = dynamic_cast<column<T> *>(ref);

		auto &data = *reinterpret_cast<const T *>(value);
		map[data].push_back(interpreted_ref->size());
		interpreted_ref->data.push_back(data);
	}

	std::size_t distinct() const override { return map.size(); }

	void do_on_matching_rows(const void *value, compare_t comp, std::function<void(std::size_t)> func) override
	{
		if (needs_to_update)
		{
			update_map();
			needs_to_update = false;
		}

		if (comp == compare_t::equals)
		{
			auto it = map.find(*reinterpret_cast<const T *>(value));
			if (it != map.end())
				for (auto i : it->second)
					func(i);
		}
		else if (comp == compare_t::greater)
		{
			auto it = map.upper_bound(*reinterpret_cast<const T *>(value));
			for (; it != map.end(); ++it)
				for (auto i : it->second)
					func(i);
		}
		else
		{
			for (auto &v : map)
				for (auto i : v.second)
					if (ref->compare(i, value) == static_cast<int>(comp))
						func(i);
		}
	}

	const std::vector<std::size_t> &get_matching_rows(const void *value, compare_t comp) override
	{
		static std::vector<std::size_t> buffer;
		buffer.clear();
		if (comp == compare_t::equals)
		{
			if (needs_to_update)
			{
				update_map();
				needs_to_update = false;
			}

			auto it = map.find(*reinterpret_cast<const T *>(value));
			if (it != map.end())
				return it->second;
			else
				return buffer;
		}
		else if (comp == compare_t::greater)
		{
			if (needs_to_update)
			{
				update_map();
				needs_to_update = false;
			}

			auto it = map.upper_bound(*reinterpret_cast<const T *>(value));
			for (; it != map.end(); ++it)
			{
				if (it->second.size() + buffer.size() > buffer.capacity())
					buffer.reserve(buffer.size() + it->second.size());
				buffer.insert(buffer.end(), it->second.begin(), it->second.end());
			}

			std::sort(buffer.begin(), buffer.end());

			return buffer;
		}
		else
		{
			buffer.reserve(ref->size());
			for (std::size_t i = 0; i < ref->size(); ++i)
				if (ref->compare(i, value) == static_cast<int>(comp))
					buffer.push_back(i);

			return buffer;
		}
	}

private:
	std::map<T, std::vector<std::size_t>> map;
	bool needs_to_update;

	void update_map()
	{
		auto interpreted_ref = dynamic_cast<column<T> *>(ref);
		map.clear();
		for (std::size_t i = 0; i < interpreted_ref->data.size(); ++i)
			map[interpreted_ref->data[i]].push_back(i);
	}
};

template <typename T>
index_base *column<T>::generate_index(index_type type)
{
	if (type == index_type::bst)
	{
		auto res = new bst_index<T>(this);
		return res;
	}
	else
	{
		auto res = new hash_index<T>(this);
		return res;
	}
}

class table
{
public:
	table() : m_index{} {}

	std::size_t generate_index(index_type type, const std::string &column)
	{
		auto it = m_column_hash.find(column);
		if (it == m_column_hash.end())
			throw std::runtime_error("column not found");

		auto col = m_columns[it->second];

		if (m_index)
		{
			if (col == m_index->ref) // if already exists there
				return m_index->distinct();
			
			delete m_index;
		}

		m_index = col->generate_index(type);

		return m_index->distinct();
	}

	// returns number of matching rows found
	std::size_t print(const std::vector<std::string> &columns, const std::string &selected_column, compare_t comp, const std::string &compare_val, bool quiet) const
	{
		auto selected_column_it = m_column_hash.find(selected_column);
		if (selected_column_it == m_column_hash.end())
			throw std::runtime_error("column not found");

		column_base &col = *m_columns[selected_column_it->second];
		void *compare_value_buff = col.buff_from_str(compare_val);

		if (quiet)
		{
			std::size_t num_matching = 0;
			if (m_index && m_index->ref == &col)
				m_index->do_on_matching_rows(compare_value_buff, comp, [&num_matching](std::size_t){ ++num_matching; });
			else
				for (std::size_t i = 0; i < col.size(); ++i)
					if (col.compare(i, compare_value_buff) == static_cast<int>(comp))
						++num_matching;
			return num_matching;
		}
		else
		{
			static std::vector<std::size_t> rows;
			rows.clear();

			rows.reserve(col.size());
			if (m_index && m_index->ref == &col)
				m_index->do_on_matching_rows(compare_value_buff, comp, [](std::size_t row){ rows.push_back(row); });
			else
				for (std::size_t i = 0; i < col.size(); ++i)
					if (col.compare(i, compare_value_buff) == static_cast<int>(comp))
						rows.push_back(i);

			for (const auto &colname : columns)
				std::cout << colname << ' ';
			std::cout << '\n';

			for (auto row : rows)
			{
				for (const auto &colname : columns)
				{
					auto it = m_column_hash.find(colname);
					if (it == m_column_hash.end())
						throw std::runtime_error("column not found");
					m_columns[it->second]->print(row);
					std::cout << ' ';
				}
				std::cout << '\n';
			}

			return rows.size();
		}
	}

	// prints all rows
	void print(const std::vector<std::string> &columns) const
	{
		for (const auto &colname : columns)
			std::cout << colname << ' ';
		std::cout << '\n';

		std::size_t N = num_rows();
		for (std::size_t i = 0; i < N; ++i)
		{
			for (const auto &colname : columns)
			{
				auto it = m_column_hash.find(colname);
				if (it == m_column_hash.end())
					throw std::runtime_error("column not found");
				m_columns[it->second]->print(i);
				std::cout << ' ';
			}
			std::cout << '\n';
		}
	}

	std::size_t delete_rows(const std::string &selected_column, compare_t comp, const std::string &compare_val)
	{
		auto selected_column_it = m_column_hash.find(selected_column);
		if (selected_column_it == m_column_hash.end())
			throw std::runtime_error("column not found");

		column_base &col = *m_columns[selected_column_it->second];
		void *compare_value_buff = col.buff_from_str(compare_val);

		const std::vector<std::size_t> *rows;
		if (m_index && m_index->ref == &col) // if on indexed column, take special care
		{
			rows = &m_index->get_matching_rows(compare_value_buff, comp);
		}
		else
		{
			static std::vector<std::size_t> buffer;
			buffer.clear();
			buffer.reserve(col.size());
			for (std::size_t i = 0; i < col.size(); ++i)
				if (col.compare(i, compare_value_buff) == static_cast<int>(comp))
					buffer.push_back(i);

			rows = &buffer;
		}

		for (auto *icol : m_columns)
		{
			if (m_index && m_index->ref == icol)
				m_index->update(true);
			icol->delete_rows(*rows);
		}

		return rows->size();
	}

	bool contains_column(const std::string &column) const
	{
		return m_column_hash.find(column) != m_column_hash.end();
	}

	void reserve_columns(std::size_t N) { m_columns.reserve(N); }

	template <typename T>
	void add_column(const std::string &colname)
	{
		m_columns.push_back(new column<T>(colname));
		m_column_hash[colname] = m_columns.size() - 1;
	}

	const std::vector<column_base *> &get_columns() const
	{
		return m_columns;
	}

	std::size_t num_cols() const { return m_columns.size(); }
	std::size_t num_rows() const
	{
		if (m_columns.empty())
			return 0;
		else
			return m_columns[0]->size();
	}

	// append value to column
	// this has faith that you add a new value to all other columns as well
	void add_value(std::size_t col, const std::string &value)
	{
		auto which = m_columns[col];

		if (m_index && which == m_index->ref)
			m_index->push_back(which->buff_from_str(value));
		else
			which->push_back(which->buff_from_str(value));
	}

	~table()
	{
		delete m_index;
		for (auto column : m_columns)
			delete column;
	}

	static std::size_t join(const table &t1, const table &t2,
							const std::string &colname1, const std::string &colname2,
							const std::vector<std::string> &columns, const std::vector<std::size_t> &which,
							bool quiet)
	{
		auto col1it = t1.m_column_hash.find(colname1);
		if (col1it == t1.m_column_hash.end())
			throw std::runtime_error("column not found");
		auto col2it = t2.m_column_hash.find(colname2);
		if (col2it == t2.m_column_hash.end())
			throw std::runtime_error("column not found");

		auto &col1 = *t1.m_columns[col1it->second];
		auto &col2 = *t2.m_columns[col2it->second];

		auto print = [&columns, &t1, &t2, quiet, &which](std::size_t i1, std::size_t i2)
		{
			if (quiet)
				return;

			for (std::size_t i = 0; i < columns.size(); ++i)
			{
				if (which[i] == 1)
				{
					auto it = t1.m_column_hash.find(columns[i]);
					if (it == t1.m_column_hash.end())
						throw std::runtime_error("column not found");
					t1.m_columns[it->second]->print(i1);
				}
				else
				{
					auto it = t2.m_column_hash.find(columns[i]);
					if (it == t2.m_column_hash.end())
						throw std::runtime_error("column not found");
					t2.m_columns[it->second]->print(i2);
				}

				std::cout << ' ';
			}

			std::cout << '\n';
		};

		if (!quiet)
		{
			for (const auto &colname : columns)
				std::cout << colname << ' ';
			std::cout << '\n';
		}

		std::size_t num_matching = 0;
		index_base *index;
		if (t2.m_index && &col2 == t2.m_index->ref)
			index = t2.m_index;
		else
			index = col2.generate_index(index_type::hash);

		for (std::size_t i1 = 0; i1 < col1.size(); ++i1)
			index->do_on_matching_rows(col1.buff_from_row(i1), compare_t::equals,
									   [i1, &print, &num_matching](std::size_t i2)
									   {
										   print(i1, i2);
										   ++num_matching;
									   });

		if (index != t2.m_index)
			delete index;

		return num_matching;
	}

private:
	std::vector<column_base *> m_columns;
	std::unordered_map<std::string, std::size_t> m_column_hash;
	index_base *m_index;
};

int main(int argc, char *argv[])
{
	std::ios_base::sync_with_stdio(false);
	std::cin >> std::boolalpha;
	std::cout << std::boolalpha;

	// Create tableOptions object to parse command line arguments
	tableOptions options(argc, argv);

	std::unordered_map<std::string, table> tables;

	enum class typenames
	{
		intname,
		stringname,
		doublename,
		boolname,
	};

	std::string cmd;
	std::string tablename1, tablename2;
	std::string tmp, tmp2, tmp3;
	std::vector<std::string> strings;
	std::vector<std::size_t> ints;
	while (std::cin)
	{

		std::cout << "% ";
		std::cin >> cmd;

		if (cmd[0] == '#')
		{
			std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
			continue;
		}
		else if (cmd == "CREATE")
		{
			std::cin >> tablename1;
			std::size_t N;
			std::cin >> N; // TODO: error checking

			if (tables.find(tablename1) != tables.end())
			{
				std::cout << "Error during CREATE: Cannot create already existing table " << tablename1 << '\n';
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cout << "New table " << tablename1 << " with column(s)";

			table &t = tables[tablename1];

			t.reserve_columns(N);
			ints.resize(N);
			for (std::size_t i = 0; i < N; ++i)
			{
				std::cin >> tmp;
				if (tmp == "double")
					ints[i] = (std::size_t)typenames::doublename;
				else if (tmp == "string")
					ints[i] = (std::size_t)typenames::stringname;
				else if (tmp == "int")
					ints[i] = (std::size_t)typenames::intname;
				else if (tmp == "bool")
					ints[i] = (std::size_t)typenames::boolname;
				else
					throw std::runtime_error("unknown type");
			}

			for (std::size_t i = 0; i < N; ++i)
			{
				std::cin >> tmp;
				std::cout << ' ' << tmp;
				switch ((typenames)ints[i])
				{
				case typenames::intname:
					t.add_column<int>(tmp);
					break;
				case typenames::stringname:
					t.add_column<std::string>(tmp);
					break;
				case typenames::doublename:
					t.add_column<double>(tmp);
					break;
				case typenames::boolname:
					t.add_column<bool>(tmp);
					break;
				}
			}

			std::cout << " created \n";
		}
		else if (cmd == "QUIT")
		{
			std::cout << "Thanks for being silly!\n";
			return 0;
		}
		else if (cmd == "REMOVE")
		{
			std::cin >> tmp;
			if (tables.erase(tmp) == 0)
			{
				std::cout << "Error during REMOVE: " << tmp << " does not name a table in the database\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
			}
			else
				std::cout << "Table " << tmp << " removed\n";
		}
		else if (cmd == "INSERT")
		{
			std::cin >> tmp;
			if (tmp != "INTO")
			{
				std::cout << "Invalid insert command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cin >> tablename1;

			auto it = tables.find(tablename1);
			if (it == tables.end())
			{
				std::cout << "Error during INSERT: " << tablename1 << " does not name a table in the database\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			table &t = it->second;

			std::size_t N;
			std::cin >> N; // TODO: error checking

			std::cin >> tmp;
			if (tmp != "ROWS")
			{
				std::cout << "Invalid insert command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::size_t K = t.num_rows();

			for (std::size_t row = 0; row < N; ++row)
			{
				for (std::size_t col = 0; col < t.num_cols(); ++col)
				{
					std::cin >> tmp;
					t.add_value(col, tmp);
				}
			}

			std::cout << "Added " << N << " rows to " << tablename1 << " from position " << K << " to " << K + N - 1 << '\n';
		}
		else if (cmd == "PRINT")
		{
			std::cin >> tmp;

			if (tmp != "FROM")
			{
				std::cout << "Invalid print command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cin >> tablename1; // tablename1
			auto it = tables.find(tablename1);
			if (it == tables.end())
			{
				std::cout << "Error during PRINT: " << tablename1 << " does not name a table in the database\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			table &t = it->second;

			std::size_t N;
			std::cin >> N; // TODO: error checking

			strings.resize(N);
			std::size_t i = 0;
			for (; i < N; ++i)
			{
				std::cin >> strings[i];
				if (!t.contains_column(strings[i]))
				{
					std::cout << "Error during PRINT: " << strings[i] << " does not name a column in " << tablename1 << '\n';
					std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
					break;
				}
			}

			if (i != N)
				continue;

			std::size_t M;

			std::cin >> tmp;
			if (tmp == "ALL")
			{
				if (!options.isQuiet())
					t.print(strings);
				
				M = t.num_rows();
			}
			else if (tmp != "WHERE")
			{
				std::cout << "Invalid print command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}
			else
			{
				std::cin >> tmp; // colname
				if (!t.contains_column(tmp))
				{
					std::cout << "Error during PRINT: " << tmp << " does not name a column in " << tablename1 << '\n';
					std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
					continue;
				}

				char op;
				std::cin >> op; // TODO: error checking

				std::cin >> tmp2; // value
				switch (op)
				{
				case '>':
					M = t.print(strings, tmp, compare_t::greater, tmp2, options.isQuiet());
					break;
				case '<':
					M = t.print(strings, tmp, compare_t::less, tmp2, options.isQuiet());
					break;
				case '=':
					M = t.print(strings, tmp, compare_t::equals, tmp2, options.isQuiet());
					break;
				default:
					std::cout << "Invalid print command\n";
					continue;
				}
			}

			std::cout << "Printed " << M << " matching rows from " << tablename1 << '\n';
		}
		else if (cmd == "DELETE")
		{
			std::cin >> tmp;
			if (tmp != "FROM")
			{
				std::cout << "Invalid DELETE command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cin >> tablename1;
			auto it = tables.find(tablename1);
			if (it == tables.end())
			{
				std::cout << "Error during DELETE: " << tablename1 << " does not name a table in the database\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			table &t = it->second;

			std::cin >> tmp;
			if (tmp != "WHERE")
			{
				std::cout << "Invalid DELETE command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cin >> tmp; // colname
			if (!t.contains_column(tmp))
			{
				std::cout << "Error during DELETE: " << tmp << " does not name a column in " << tablename1 << '\n';
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			char op;
			std::cin >> op; // TODO: error checking

			std::cin >> tmp2; // value

			std::size_t N;
			switch (op)
			{
			case '>':
				N = t.delete_rows(tmp, compare_t::greater, tmp2);
				break;
			case '<':
				N = t.delete_rows(tmp, compare_t::less, tmp2);
				break;
			case '=':
				N = t.delete_rows(tmp, compare_t::equals, tmp2);
				break;
			default:
				std::cout << "Invalid print command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cout << "Deleted " << N << " rows from " << tablename1 << '\n';
		}
		else if (cmd == "JOIN")
		{
			static std::vector<std::pair<std::string, int>> print_columns;
			print_columns.clear();

			std::cin >> tablename1;
			auto it = tables.find(tablename1);
			if (it == tables.end())
			{
				std::cout << "Error during JOIN: " << tablename1 << " does not name a table in the database\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}
			table &t1 = it->second;

			std::cin >> tmp;
			if (tmp != "AND")
			{
				std::cout << "Invalid JOIN command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cin >> tablename2;
			it = tables.find(tablename2);
			if (it == tables.end())
			{
				std::cout << "Error during JOIN: " << tablename2 << " does not name a table in the database\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}
			table &t2 = it->second;

			std::cin >> tmp;
			if (tmp != "WHERE")
			{
				std::cout << "Invalid JOIN command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::string colname1;
			std::cin >> colname1;
			if (!t1.contains_column(colname1))
			{
				std::cout << "Error during JOIN: " << colname1 << " does not name a column in " << tablename1 << '\n';
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			char tmp_char;
			std::cin >> tmp_char;

			std::string colname2;
			std::cin >> colname2;
			if (!t2.contains_column(colname2))
			{
				std::cout << "Error during JOIN: " << colname2 << " does not name a column in " << tablename2 << '\n';
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cin >> tmp;
			if (tmp != "AND")
			{
				std::cout << "Invalid JOIN command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}
			std::cin >> tmp;
			if (tmp != "PRINT")
			{
				std::cout << "Invalid JOIN command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::size_t N;
			std::cin >> N;

			strings.resize(N);
			ints.resize(N);

			std::size_t i = 0;
			for (; i < N; ++i)
			{
				std::cin >> strings[i];
				std::cin >> ints[i];

				if (ints[i] == 1)
				{
					if (!t1.contains_column(strings[i]))
					{
						std::cout << "Error during JOIN: " << strings[i] << " does not name a column in " << tablename1 << '\n';
						break;
					}
				}
				else if (ints[i] == 2)
				{
					if (!t2.contains_column(strings[i]))
					{
						std::cout << "Error during JOIN: " << strings[i] << " does not name a column in " << tablename2 << '\n';
						break;
					}
				}
				else
				{
					std::cout << "Invalid print command\n";
					break;
				}
			}

			if (i != N)
			{
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			N = table::join(t1, t2, colname1, colname2, strings, ints, options.isQuiet());

			std::cout << "Printed " << N << " rows from joining " << tablename1 << " to " << tablename2 << '\n';
		}
		else if (cmd == "GENERATE")
		{
			std::cin >> tmp;
			if (tmp != "FOR")
			{
				std::cout << "Invalid GENERATE command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cin >> tablename1;
			auto it = tables.find(tablename1);
			if (it == tables.end())
			{
				std::cout << "Error during GENERATE: " << tablename1 << " does not name a table in the database\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			table &t = it->second;

			std::cin >> tmp; // indextype

			index_type type = tmp == "bst" ? index_type::bst : index_type::hash;

			std::cin >> tmp2;
			if (tmp2 != "INDEX")
			{
				std::cout << "Invalid GENERATE command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cin >> tmp2;
			if (tmp2 != "ON")
			{
				std::cout << "Invalid GENERATE command\n";
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::cin >> tmp2; // colname
			if (!t.contains_column(tmp2))
			{
				std::cout << "Error during GENERATE: " << tmp2 << " does not name a column in " << tablename1 << '\n';
				std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
				continue;
			}

			std::size_t N = t.generate_index(type, tmp2);

			std::cout << "Created " << tmp << " index for table " << tablename1 << " on column " << tmp2 << ", with " << N << " distinct keys\n";
		}
		else
		{
			std::cout << "Error: unrecognized command\n";
			std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
		}
	}
}