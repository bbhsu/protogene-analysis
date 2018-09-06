% analysis_pipeline_sql
% Statistical analysis pipeline used to identify beneficial/neutral/deleterious
% strains based on colony-size measurements.
% Input: Raw colony sizes
$ Output: Fitness, stats, p-value, q-value, effect (beneficial/neutral/deleterious)
% Brian Hsu, 2015

javaaddpath(‘/home/sbp29/MATLAB/mysql-connector-java-5.1.45-bin.jar’)
addpath(genpath('/home/sbp29/MATLAB/Matlab-Colony-Analyzer-Toolkit-master'))
addpath(genpath('/home/sbp29/MATLAB/bean-matlab-toolkit-master'))
addpath('/home/bhsu/data_raw2sql/sql_functions')
addpath('/home/bhsu/data_raw2sql/sql_functions/utilities')

%% Declare tables and SQL connection

%Set preferences with setdbprefs.
setdbprefs('DataReturnFormat', 'structure');
setdbprefs('NullNumberRead', 'NaN');
setdbprefs('NullStringRead', 'null');

database_name = '';
username = '';
password = '';
address = '';

conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

%% Choose subset of tables to upload

table_ind = [];

table_fields = fieldnames(table_data);

for ii = 1 : length(table_fields)
	table_data.(table_fields{ii}) = table_data.(table_fields{ii})(table_ind);
end
tables = table_data.table_name;

%% Get indices to zero from source plates

zero_ind = cell(1, length(tables));
for ii = 1 : length(table_data.source)
	tic;
  zero_ind{ii} = zero_data_from_source(table_data.source{ii}, conn);
  toc;
end

%% Convert JPEG_RESULTS to SPATIAL_RESULTS

for ii = 1 : length(tables)

	foo_table = strcat('JPEG_RESULTS_v2_', tables{ii});
  foo1_table = strcat('SPATIAL_RESULTS_v2_', tables{ii});
  exec(conn, ['drop table ' foo1_table]);
  exec(conn, ['create table ' foo1_table ' (pos int not null, pid int not null,' ...
      ' exp_id int not null, hours int not null, replicate1 int null, replicate2 int null,' ...
      ' replicate3 int null, average double null, csS double null, csM double null)']);

  curs = exec(conn, ['SELECT distinct exp_id from ' foo_table]);
	curs = fetch(curs);
	close(curs);
	jpeg_data = curs.Data;
	all_exp = jpeg_data.exp_id;

    for ee = 1 : length(all_exp)

        curs = exec(conn, ['SELECT distinct hours from ' foo_table ' where exp_id = ' num2str(all_exp(ee))]);
        curs = fetch(curs);
        close(curs);
        hours_data = curs.Data;
        all_hours = hours_data.hours;

        for jj = 1 : length(all_hours)
            tic;
            curs = exec(conn, ['SELECT * from ' foo_table ' where hours = ' num2str(all_hours(jj)) ...
                ' and exp_id = ' num2str(all_exp(ee))]);
            curs = fetch(curs);
            close(curs);
            data_all = curs.Data;

            % Clean data
            [n_plate, density] = detect_density(data_all.pid);
            rep_data = [data_all.replicate1, data_all.replicate2, data_all.replicate3];
            [cleaned_rep_data, data_all.average] = ...
                clean_raw_data(rep_data, n_plate, density);
            data_all.replicate1 = cleaned_rep_data(:, 1);
            data_all.replicate2 = cleaned_rep_data(:, 2);
            data_all.replicate3 = cleaned_rep_data(:, 3);

            % NaN out zeros from source plate
            data_all.average(zero_ind{ii}) = NaN;
            data_all.csS(zero_ind{ii}) = NaN;
            data_all.csM(zero_ind{ii}) = NaN;

            data_all_copy = data_all;
            pid_all = data_all.pid;

            % Swap all values in FLAGS where act is NOT NULL
            curs = exec(conn, ['SELECT * from FLAGS_ALL where exp_id = ' num2str(all_exp(ee)) ...
                ' and hours = ' num2str(all_hours(jj)) ' and act is not null order by pid asc']);
            curs = fetch(curs);
            close(curs);
            swap_data = curs.Data;

            if ~iscell(swap_data)
                pid_swap = swap_data.pid;
                pid_swapto = swap_data.act;
                [las1, lbs1] = ismember(pid_swap, pid_all);
                [las2, lbs2] = ismember(pid_swapto, pid_all);

                data_all.replicate1(lbs1) = data_all_copy.replicate1(lbs2);
                data_all.replicate2(lbs1) = data_all_copy.replicate2(lbs2);
                data_all.replicate3(lbs1) = data_all_copy.replicate3(lbs2);
                data_all.average(lbs1) = data_all_copy.average(lbs2);
                data_all.csS(lbs1) = data_all_copy.csS(lbs2);
                data_all.csM(lbs1) = data_all_copy.csM(lbs2);
            end

            %NULL all values in FLAGS where act is NULL
            curs = exec(conn, 'SELECT * from FLAGS_ALL where act is null');
            curs = fetch(curs);
            close(curs);
            null_data = curs.Data;
            pid_null = null_data.pid;

            [la, lb] = ismember(pid_null, pid_all);
            pid_nan = lb(la);
            data_all.average(pid_nan) = NaN;
            data_all.csS(pid_nan) = NaN;
            data_all.csM(pid_nan) = NaN;

            untitled2 = struct2array(data_all);
            values = cell(1, size(data_all.pos, 1));
            for uu = 1 : size(data_all.pos, 1)
                values{uu} = sprintf('(%d,%d,%d,%d,%d,%d,%d,%f,%f,%f),', untitled2(uu, :));
            end

            values = regexprep(values, 'NaN', 'NULL');
            values{end}(end) = '';
            ins_query = sprintf('insert into %s (pos, pid, exp_id, hours, replicate1, replicate2, replicate3, average, csS, csM) values ', foo1_table);
            exec(conn, [ins_query, values{:}]);

            toc;
        end
    end
    disp([tables{ii} 'SPATIAL RESULTS DONE']);
end
close(conn);
clearvars -except tables database_name username password address
disp('SPATIAL RESULTS DONE');
%% SPATIAL_RESULTS -> FITNESS w/ identifiers
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'orf_name', 'pid', 'exp_id', 'hours', 'is_orf', 'average', 'fitness'};
col_format = '"%s",%d,%d,%d,%d,%f,%f';

for ii = 1 : length(tables)
    foo_table = strcat('SPATIAL_RESULTS_v2_', tables{ii});
    foo1_table = strcat('FITNESS_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (orf_name varchar(255) null, pid int not null,' ...
        ' exp_id int not null, hours int null, is_orf int null, average double null, fitness double null)']);

    data = calculate_fitness(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('FITNESS DONE');
%% FITNESS -> ANALYSIS
% stats, p-values, q-values, fitness threshold
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'orf_name', 'exp_id', 'hours', 'mean', 'median'};
col_format = '"%s",%d,%d,%f,%f';

for ii = 1 : length(tables)
    foo_table = strcat('FITNESS_v2_', tables{ii});
    foo1_table = strcat('STATS_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (orf_name varchar(255) null, exp_id int not null,' ...
        ' hours int not null, mean double null, median double null)']);

    data = calculate_stats(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('STATS DONE');
%% FITNESS -> p-values
% stats, p-values, q-values, fitness threshold
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'orf_name', 'exp_id', 'hours', 'p', 'stat'};
col_format = '"%s",%d,%d,%f,%f';

for ii = 1 : length(tables)
    foo_table = strcat('FITNESS_v2_', tables{ii});
    foo1_table = strcat('PVALUES_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (orf_name varchar(255) null, exp_id int not null,' ...
        ' hours int not null, p double null, stat double null)']);

    data = mann_whitney(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('P-VALUES DONE');
%% FITNESS -> q-values
% stats, p-values, q-values, fitness threshold
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'orf_name', 'exp_id', 'hours', 'q', 'p', 'stat'};
col_format = '"%s",%d,%d,%f,%f,%f';

for ii = 1 : length(tables)
    foo_table = strcat('PVALUES_v2_', tables{ii});
    foo1_table = strcat('QVALUES_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (orf_name varchar(255) null, exp_id int not null,' ...
        ' hours int not null, q double null, p double null, stat double null)']);

    data = qvalue_correction(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('Q-VALUES DONE');
%% FITNESS -> perc
% stats, p-values, q-values, fitness threshold
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'exp_id', 'hours', 'perc5', 'perc95'};
col_format = '%d,%d,%f,%f';

for ii = 1 : length(tables)
    foo_table = strcat('FITNESS_v2_', tables{ii});
    foo1_table = strcat('PERC_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (exp_id int not null,' ...
        ' hours int not null, perc5 double null, perc95 double null)']);

    data = calculate_percentile(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('PERC DONE');
%% SPATIAL RESULTS -> GROWTH RATE
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'pos', 'pid', 'exp_id', 'hours', 'GR'};
col_format = '%d,%d,%d,%d,%f';

for ii = 1 : length(tables)
    foo_table = strcat('SPATIAL_RESULTS_v2_', tables{ii});
    foo1_table = strcat('GROWTHRATE_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (pos int not null, pid int not null, exp_id int not null,' ...
        ' hours int not null, GR double null)']);

    data = calculate_growth_rate(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('GR SPATIAL RESULTS DONE');
%% GROWTH RATE -> GROWTH RATE FITNESS w/ identifiers
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'orf_name', 'pid', 'exp_id', 'hours', 'is_orf', 'fitness'};
col_format = '"%s",%d,%d,%d,%d,%f';

for ii = 1 : length(tables)
    foo_table = strcat('GROWTHRATE_v2_', tables{ii});
    foo1_table = strcat('GR_FITNESS_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (orf_name varchar(255) null, pid int not null,' ...
        ' exp_id int not null, hours int null, is_orf int null, fitness double null)']);

    data = calculate_growth_fitness(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('GR FITNESS DONE');
%% GROWTH RATE FITNESS -> ANALYSIS
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'orf_name', 'exp_id', 'hours', 'mean', 'median'};
col_format = '"%s",%d,%d,%f,%f';

for ii = 1 : length(tables)
    foo_table = strcat('GR_FITNESS_v2_', tables{ii});
    foo1_table = strcat('GR_STATS_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (orf_name varchar(255) null, exp_id int not null,' ...
        ' hours int not null, mean double null, median double null)']);

    data = calculate_stats(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('GR STATS DONE');
%% GROWTH RATE FITNESS -> P-VALUES
% stats, p-values, q-values, fitness threshold
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'orf_name', 'exp_id', 'hours', 'p', 'stat'};
col_format = '"%s",%d,%d,%f,%f';

for ii = 1 : length(tables)
    foo_table = strcat('GR_FITNESS_v2_', tables{ii});
    foo1_table = strcat('GR_PVALUES_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (orf_name varchar(255) null, exp_id int not null,' ...
        ' hours int not null, p double null, stat double null)']);

    data = mann_whitney(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('GR P-VALUES DONE');
%% GROWTH RATE P-VALUES -> Q-VALUES
% stats, p-values, q-values, fitness threshold
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'orf_name', 'exp_id', 'hours', 'q', 'p', 'stat'};
col_format = '"%s",%d,%d,%f,%f,%f';

for ii = 1 : length(tables)
    foo_table = strcat('GR_PVALUES_v2_', tables{ii});
    foo1_table = strcat('GR_QVALUES_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (orf_name varchar(255) null, exp_id int not null,' ...
        ' hours int not null, q double null, p double null, stat double null)']);

    data = qvalue_correction(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('GR Q-VALUES DONE');
%% GROWTH RATE FITNESS -> PERC
% stats, p-values, q-values, fitness threshold
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

col_names = {'exp_id', 'hours', 'perc5', 'perc95'};
col_format = '%d,%d,%f,%f';

for ii = 1 : length(tables)
    foo_table = strcat('GR_FITNESS_v2_', tables{ii});
    foo1_table = strcat('GR_PERC_v2_', tables{ii});
    exec(conn, ['drop table ' foo1_table]);
    exec(conn, ['create table ' foo1_table ' (exp_id int not null,' ...
        ' hours int not null, perc5 double null, perc95 double null)']);

    data = calculate_percentile(conn, foo_table);

    exps = fieldnames(data);
    for jj = 1 : length(exps)
        fasterinsert(conn, data.(exps{jj}), col_names, col_format, foo1_table);
    end
end
close(conn);
clearvars -except tables database_name username password address
disp('GR PERC DONE');
%% Insert individual tables into main GR tables
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

data_tables = {'GR_STATS_ALL', 'GR_QVALUES_ALL', 'GR_PERC_ALL', 'GR_DATASET'};
temp_tables = cellfun( @(x) strcat(x, '_temp'), data_tables, 'Uniformoutput', false);
for ii = 1 : length(data_tables)
    mysql_table = sql_query(conn, ['show tables like "' data_tables{ii} '"']);
    if iscell(mysql_table) && strcmpi(mysql_table{1}, 'No Data') == 1
        disp('Making new tables.');
        if ii == 1
            exec(conn, ['create table ' data_tables{ii} ' (orf_name varchar(255) not null, exp_id int not null, hours int not null, mean double null, median double null)']);
            exec(conn, ['alter table ' data_tables{ii} ' add primary key(orf_name, exp_id, hours)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(mean)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(median)']);
        elseif ii == 2
            exec(conn, ['create table ' data_tables{ii} ' (orf_name varchar(255) not null, exp_id int not null, hours int not null, q double null, p double null, stat double null)']);
            exec(conn, ['alter table ' data_tables{ii} ' add primary key(orf_name, exp_id, hours)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(q)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(p)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(stat)']);
        elseif ii == 3
            exec(conn, ['create table ' data_tables{ii} ' (exp_id int not null, hours int not null, perc5 double null, perc95 double null)']);
            exec(conn, ['alter table ' data_tables{ii} ' add primary key(exp_id, hours)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(perc5)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(perc95)']);
        elseif ii == 4
            exec(conn, ['create table ' data_tables{ii} ' (orf_name varchar(255) not null, exp_id int not null, hours int not null, median double null, q double null, p double null, stat double null, effect varchar(255) not null)']);
            exec(conn, ['alter table ' data_tables{ii} ' add primary key(orf_name, exp_id, hours)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(median)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(q)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(p)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(stat)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(effect)']);
        end
    end
end

for ii = 1 : length(tables)

    exp_data = sql_query(conn, ['select distinct exp_id from GR_PERC_v2_' tables{ii}]);
    exp_str = strcat('(', regexprep(num2str(exp_data.exp_id'), '\s+', ','), ')');

    tic;
    for jj = 1 : length(data_tables)
        exec(conn, ['delete from ' data_tables{jj} ' where exp_id in ' exp_str]);
        exec(conn, ['drop table ' temp_tables{jj}]);
    end
    toc;

    tic;
    exec(conn, ['create table ' temp_tables{1} ' select * from GR_STATS_v2_' tables{ii}]);
    exec(conn, ['create table ' temp_tables{2} ' select * from GR_QVALUES_v2_', tables{ii}]);
    exec(conn, ['create table ' temp_tables{3} ' select * from GR_PERC_v2_', tables{ii}]);

    exec(conn, ['alter table ' temp_tables{1} ' add primary key(orf_name, exp_id, hours)']);
    exec(conn, ['alter table ' temp_tables{1} ' add index(mean)']);
    exec(conn, ['alter table ' temp_tables{1} ' add index(median)']);
    exec(conn, ['alter table ' temp_tables{2} ' add primary key(orf_name, exp_id, hours)']);
    exec(conn, ['alter table ' temp_tables{2} ' add index(q)']);
    exec(conn, ['alter table ' temp_tables{2} ' add index(p)']);
    exec(conn, ['alter table ' temp_tables{2} ' add index(stat)']);
    exec(conn, ['alter table ' temp_tables{3} ' add primary key(exp_id, hours)']);
    exec(conn, ['alter table ' temp_tables{3} ' add index(perc5)']);
    exec(conn, ['alter table ' temp_tables{3} ' add index(perc95)']);
    toc;

    tic;
    exec(conn, ['create table ' temp_tables{4} ' (select a.orf_name, a.exp_id, a.hours, a.median, b.q, b.p, b.stat from ' temp_tables{1} ' a, ' temp_tables{2} ' b where a.orf_name=b.orf_name and a.exp_id=b.exp_id and a.hours=b.hours)']);
    exec(conn, ['alter table ' temp_tables{4} ' add effect varchar(255) not null']);

    exec(conn, ['alter table ' temp_tables{4} ' add primary key(orf_name, exp_id, hours)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(median)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(q)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(p)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(stat)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(effect)']);

    exec(conn, ['update ' temp_tables{4} ' a, ' temp_tables{3} ' b set effect="deleterious" where a.exp_id=b.exp_id and median<perc5 and q<0.05']);
    exec(conn, ['update ' temp_tables{4} ' a, ' temp_tables{3} ' b set effect="beneficial" where a.exp_id=b.exp_id and median>perc95 and q<0.05']);
    exec(conn, ['update ' temp_tables{4} ' set effect="neutral" where effect = ""']);
    toc;

    tic;
    for jj = 1 : length(data_tables)
        exec(conn, ['insert into ' data_tables{jj} ' select * from ' temp_tables{jj}]);
        exec(conn, ['drop table ' temp_tables{jj}]);
    end
    toc;
end

%% Insert individual table into main tables
conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

data_tables = {'STATS_ALL5', 'QVALUES_ALL5', 'PERC_ALL5', 'DATASET_5', 'FITNESS_ALL5'};
temp_tables = cellfun( @(x) strcat(x, '_temp'), data_tables, 'Uniformoutput', false);
for ii = 1 : length(data_tables)
    mysql_table = sql_query(conn, ['show tables like "' data_tables{ii} '"']);
    if iscell(mysql_table) && strcmpi(mysql_table{1}, 'No Data') == 1
        if ii == 1
            exec(conn, ['create table ' data_tables{ii} ' (orf_name varchar(255) not null, exp_id int not null, hours int not null, mean double null, median double null)']);
            exec(conn, ['alter table ' data_tables{ii} ' add primary key(orf_name, exp_id, hours)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(mean)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(median)']);
        elseif ii == 2
            exec(conn, ['create table ' data_tables{ii} ' (orf_name varchar(255) not null, exp_id int not null, hours int not null, q double null, p double null, stat double null)']);
            exec(conn, ['alter table ' data_tables{ii} ' add primary key(orf_name, exp_id, hours)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(q)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(p)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(stat)']);
        elseif ii == 3
            exec(conn, ['create table ' data_tables{ii} ' (exp_id int not null, hours int not null, perc5 double null, perc95 double null)']);
            exec(conn, ['alter table ' data_tables{ii} ' add primary key(exp_id, hours)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(perc5)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(perc95)']);
        elseif ii == 4
            exec(conn, ['create table ' data_tables{ii} ' (orf_name varchar(255) not null, exp_id int not null, hours int not null, N int not null, colony_size double null, q_cs double null, growth_rate double null, q_gr double null, effect_cs varchar(255) not null, effect_gr varchar(255) not null)']);
            exec(conn, ['alter table ' data_tables{ii} ' add primary key(orf_name, exp_id, hours)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(N)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(colony_size)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(growth_rate)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(q_cs)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(q_gr)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(effect_cs)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(effect_gr)']);
        elseif ii == 5
            exec(conn, ['create table ' data_tables{ii} ' (orf_name varchar(255) not null, pid int not null, exp_id int not null, hours int null, is_orf int null, average double null, fitness double null)']);
            exec(conn, ['alter table ' data_tables{ii} ' add primary key(pid, exp_id)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(hours)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(orf_name)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(is_orf)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(average)']);
            exec(conn, ['alter table ' data_tables{ii} ' add index(fitness)']);
        end
    end
end

for ii = 1 : length(tables)

    exp_data = sql_query(conn, ['select distinct exp_id from PERC_v2_' tables{ii}]);
    exp_str = strcat('(', regexprep(num2str(exp_data.exp_id'), '\s+', ','), ')');

    tic;
    for jj = 1 : length(data_tables)
        exec(conn, ['delete from ' data_tables{jj} ' where exp_id in ' exp_str]);
        exec(conn, ['drop table ' temp_tables{jj}]);
    end
    toc;

    tic;
    exec(conn, ['insert into ' data_tables{5} ' select * from FITNESS_v2_', tables{ii}]);
    toc;

    tic;
    exec(conn, ['create table ' temp_tables{1} ' select * from STATS_v2_' tables{ii}]);
    exec(conn, ['create table ' temp_tables{2} ' select * from QVALUES_v2_', tables{ii}]);
    exec(conn, ['create table ' temp_tables{3} ' select * from PERC_v2_', tables{ii}]);

    exec(conn, ['alter table ' temp_tables{1} ' add primary key(orf_name, exp_id, hours)']);
    exec(conn, ['alter table ' temp_tables{1} ' add index(mean)']);
    exec(conn, ['alter table ' temp_tables{1} ' add index(median)']);
    exec(conn, ['alter table ' temp_tables{2} ' add primary key(orf_name, exp_id, hours)']);
    exec(conn, ['alter table ' temp_tables{2} ' add index(q)']);
    exec(conn, ['alter table ' temp_tables{2} ' add index(p)']);
    exec(conn, ['alter table ' temp_tables{2} ' add index(stat)']);
    exec(conn, ['alter table ' temp_tables{3} ' add primary key(exp_id, hours)']);
    exec(conn, ['alter table ' temp_tables{3} ' add index(perc5)']);
    exec(conn, ['alter table ' temp_tables{3} ' add index(perc95)']);
    toc;

    tic;
    exec(conn, ['create table ' temp_tables{4} ' (select a.orf_name, a.exp_id, a.hours, a.median as colony_size, b.q as q_cs from ' temp_tables{1} ' a, ' temp_tables{2} ' b where a.orf_name=b.orf_name and a.exp_id=b.exp_id and a.hours=b.hours)']);

    exec(conn, ['alter table ' temp_tables{4} ' add N int not null after hours']);
    exec(conn, ['alter table ' temp_tables{4} ' add growth_rate double null']);
    exec(conn, ['alter table ' temp_tables{4} ' add q_gr double null']);
    exec(conn, ['alter table ' temp_tables{4} ' add effect_cs varchar(255) not null']);
    exec(conn, ['alter table ' temp_tables{4} ' add effect_gr varchar(255) not null']);

    exec(conn, ['alter table ' temp_tables{4} ' add primary key(orf_name, exp_id, hours)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(N)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(colony_size)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(q_cs)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(growth_rate)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(q_gr)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(effect_cs)']);
    exec(conn, ['alter table ' temp_tables{4} ' add index(effect_gr)']);

    exec(conn, ['update ' temp_tables{4} ' a, ' temp_tables{3} ' b set effect_cs="deleterious" where a.exp_id=b.exp_id and colony_size<perc5 and q_cs<0.01']);
    exec(conn, ['update ' temp_tables{4} ' a, ' temp_tables{3} ' b set effect_cs="beneficial" where a.exp_id=b.exp_id and colony_size>perc95 and q_cs<0.01']);
    exec(conn, ['update ' temp_tables{4} ' set effect_cs="neutral" where effect_cs = ""']);
    exec(conn, ['update ' temp_tables{4} ' a, (select orf_name, exp_id, hours, count(orf_name) n from ' data_tables{5} ' where fitness is not null group by orf_name, exp_id) b set a.N=b.n where a.orf_name=b.orf_name and a.exp_id=b.exp_id']);
    toc;

    tic;
    for jj = 1 : length(data_tables) - 1
        exec(conn, ['insert into ' data_tables{jj} ' select * from ' temp_tables{jj}])
        exec(conn, ['drop table ' temp_tables{jj}])
    end
    toc;
end
