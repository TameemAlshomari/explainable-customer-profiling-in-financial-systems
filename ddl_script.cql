create keyspace if not exists customer_profiling with replication =
    {'class': 'SimpleStrategy', 'replication_factor': 1};

create table if not exists customer_profiling.customer_login
(
    id         int primary key,
    last_login timestamp,
    party_id   varint
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
     and compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
     and dclocal_read_repair_chance = 0.1;

create table if not exists customer_profiling.invoices
(
    party_id               varint primary key,
    avg_inv_period         float,
    avg_taxexclusiveamount float,
    avg_taxinclusiveamount float,
    count                  varint,
    curr                   list<text>,
    currency_vec           text,
    max_inv_period         float,
    max_taxexclusiveamount float,
    max_taxinclusiveamount float,
    min_inv_period         float,
    min_taxexclusiveamount float,
    min_taxinclusiveamount float,
    sum_taxexclusiveamount float,
    sum_taxinclusiveamount float
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
     and compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
     and dclocal_read_repair_chance = 0.1;

create table if not exists customer_profiling.predictions
(
    features   text primary key,
    prediction float
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
     and compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
     and dclocal_read_repair_chance = 0.1;

create table if not exists customer_profiling.static_data
(
    party_id varint primary key,
    county   int,
    features text,
    "index"  bigint,
    lob_code varint,
    package  int,
    region   int,
    size     int
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
     and compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
     and dclocal_read_repair_chance = 0.1;

create table if not exists customer_profiling.transfer_history
(
    party_id               varint primary key,
    avg_amount             float,
    count                  int,
    max_amount             float,
    min_amount             float,
    sum_amount             float,
    transfer_curr          list<text>,
    transfer_currency_vec  text,
    uniq_transfer_id_count int
)
    with caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
     and compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
     and compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
     and dclocal_read_repair_chance = 0.1;

