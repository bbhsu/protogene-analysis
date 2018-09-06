% upload_raw_cs_to_sql
% Upload raw colony sizes to SQL database.
% Brian Hsu, 2015

javaaddpath(‘/home/sbp29/MATLAB/mysql-connector-java-5.1.45-bin.jar’)
addpath(genpath('/home/sbp29/MATLAB/Matlab-Colony-Analyzer-Toolkit-master'))
addpath(genpath('/home/sbp29/MATLAB/bean-matlab-toolkit-master'))
addpath('/home/bhsu/data_raw2sql/sql_functions')
addpath('/home/bhsu/data_raw2sql/sql_functions/utilities')

%% List all tables

table_file = readtable('~/yeast_beneficial_data/EXPS_data.csv');

%% Get file paths and parameter data for SQL raw data upload

text_path = '~/yeast_beneficial_data/sql_data/raw/';

for ii = 1 : length(table_file.table)
    table_data.table_name(ii) = table_file.table(ii);
    table_data.path(ii) = table_file.path(ii);
    table_data.source(ii) = table_file.source(ii);
    table_data.textfile{ii} = strcat(text_path, table_data.table_name{ii},'.txt');

    if strfind(table_file.exp_id{ii}, '-')
        exps = strsplit(table_file.exp_id{ii}, '-');
        table_data.exp_id{ii} = str2double(exps{1}):str2double(exps{2});
    else
        table_data.exp_id{ii} = cellfun( @str2num, strsplit(table_file.exp_id{ii}, '|'));
    end

    table_data.density(ii) = 6144;
    table_data.n_row(ii) = 64;
    table_data.id(ii) = table_file.pid(ii);
    if table_data.id(ii) < 300000
        table_data.n_plate(ii) = 5;
    else
        table_data.n_plate(ii) = 4;
    end

    data = dir(table_file.path{ii});
    hours_folders = {data.name};
    hours_folders = hours_folders(cellfun(@(x) ~isempty(strfind(x, 'hrs')) & length(x) < 7, hours_folders, 'UniformOutput', 1)==1);
    table_data.all_hours{ii} = sort(str2double(cellfun(@(x) x(1:strfind(x, 'hrs')-1), hours_folders, 'UniformOutput', 0)'));

    data = dir(fullfile(table_file.path{ii}, hours_folders{1}));
    set1_folders = {data.name};
    set1_folders = set1_folders(cellfun(@(x) ~isempty(strfind(x, 'set')) & length(x) < 7, set1_folders, 'UniformOutput', 1)==1);

    table_data.main_set{ii} = {};
    table_data.sub_set{ii} = {};
    for jj = 1 : length(set1_folders)
        data = dir(fullfile(table_file.path{ii}, hours_folders{1}, set1_folders{jj}));
        set2_folders = {data.name};
        set2_folders = set2_folders(cellfun(@(x) ~isempty(strfind(x, 'set')) & length(x) < 7, set2_folders, 'UniformOutput', 1)==1);
        table_data.main_set{ii} = vertcat(table_data.main_set{ii}, repmat(set1_folders(jj), length(set2_folders), 1));
        table_data.sub_set{ii} = vertcat(table_data.sub_set{ii}, set2_folders');
    end

    for hh = 1 : length(hours_folders)
        data = dir(fullfile(table_file.path{ii}, hours_folders{hh}));
        set1_folders = {data.name};
        set1_folders = set1_folders(cellfun(@(x) ~isempty(strfind(x, 'set')) & length(x) < 7, set1_folders, 'UniformOutput', 1)==1);
        if length(set1_folders) ~= length(unique(table_data.main_set{ii}))
            hrs = strsplit(hours_folders{hh}, 'hrs');
            hrs = str2double(hrs{1});
            table_data.all_hours{ii}(table_data.all_hours{ii} == hrs) = [];
        end
        for jj = 1 : length(set1_folders)
            data = dir(fullfile(table_file.path{ii}, hours_folders{hh}, set1_folders{jj}));
            set2_folders = {data.name};
            set2_folders = set2_folders(cellfun(@(x) ~isempty(strfind(x, 'set')) & length(x) < 7, set2_folders, 'UniformOutput', 1)==1);
            if length(set2_folders) ~= length(unique(table_data.sub_set{ii}(strcmpi(table_data.main_set{ii}, set1_folders{jj})==1)))
                hrs = strsplit(hours_folders{hh}, 'hrs');
                hrs = str2double(hrs{1});
                table_data.all_hours{ii}(table_data.all_hours{ii} == hrs) = [];
            end
            for kk = 1 : length(set2_folders)
                data = dir(fullfile(table_file.path{ii}, hours_folders{hh}, set1_folders{jj}, set2_folders{kk}));
                files = {data.name};
                files = files(cellfun(@(x) ~isempty(strfind(x, '.binary')), files, 'UniformOutput', 1)==1);
                if length(files) < 12
                    hrs = strsplit(hours_folders{hh}, 'hrs');
                    hrs = str2double(hrs{1});
                    table_data.all_hours{ii}(table_data.all_hours{ii} == hrs) = [];
                end
            end
        end
    end
end
clearvars -except table_data table_file
%% MySQL parameters

%Set preferences with setdbprefs.
setdbprefs('DataReturnFormat', 'structure');
setdbprefs('NullNumberRead', 'NaN');
setdbprefs('NullStringRead', 'null');

%Make connection to database.  Note that the password has been omitted.
%Using JDBC driver.
database_name = '';
username = '';
password = '';
address = '';

conn = database(database_name, username, password, 'Vendor', 'MYSQL', 'Server', address);

%% Write data to text file and insert into individual JPEG_RESULTS

for tt = 1 : length(table_data.table_name)
    foo_table = strcat('JPEG_RESULTS_v2_', table_data.table_name{tt});

    exec(conn, ['drop table ' foo_table]);
    exec(conn, ['create table ' foo_table ' (pos int not null, pid int not null,' ...
        ' exp_id int not null, hours int not null, replicate1 int null, replicate2 int null,' ...
        ' replicate3 int null, average double null, csS double null, csM double null)']);

    all_hours = table_data.all_hours{tt};
    mainset = table_data.main_set{tt};
    subset = table_data.sub_set{tt};
    expid = table_data.exp_id{tt};
    n_plate = table_data.n_plate(tt);
    density = table_data.density(tt);
    n_row = table_data.n_row(tt);
    id = table_data.id(tt);
    textfile = table_data.textfile{tt};

    for hrs = 1 : length(all_hours)
        for s = 1 : length(mainset)
            tic;

            hrsid = sprintf('hrs%d', all_hours(hrs));
            setid = sprintf('%s_%s', mainset{s}, subset{s});

            % load colony information
            imagedir = sprintf(strcat(table_data.path{tt}, '/%dhrs/%s/%s'), all_hours(hrs), mainset{s}, subset{s});
            cs.(hrsid).(setid).with_image_reps = load_colony_sizes(imagedir);

            % Average data across three image replicates
            cs.(hrsid).(setid).raw = squeeze( ...
                mean(reshape(cs.(hrsid).(setid).with_image_reps, [3 n_plate density]),1));

            % Temporarily NaN zeroes (raw colony size < 10 is NaN)
            the_zeros = cs.(hrsid).(setid).raw < 10;
            cs.(hrsid).(setid).nanned_zeros = fil(cs.(hrsid).(setid).raw, the_zeros);

            % Spatially correct average colony size with border correction,
            % spatial correction, and mode normalization
            cs.(hrsid).(setid).spatial = apply_correction( ...
                cs.(hrsid).(setid).nanned_zeros, 'dim', 2, ...
                InterleaveFilter(SpatialBorderMedian('SpatialFilter', ...
                SpatialMedian('windowSize', 9))), ...
                PlateMode() );

            cs.(hrsid).(setid).spatial_median = apply_correction( ...
                cs.(hrsid).(setid).nanned_zeros, 'dim', 2, ...
                'function', @(x, b) x ./ b .* nanmedian(x(:)), ...
                InterleaveFilter(SpatialBorderMedian('SpatialFilter', ...
                SpatialMedian('windowSize', 9))), ...
                PlateMode() );

            % Here, the NaNs that are not in the H2 box are changed back to 0s
            cs.(hrsid).(setid).spatial(the_zeros) = 0;
            cs.(hrsid).(setid).spatial_median(the_zeros) = 0;

            CS = cs.(hrsid).(setid).with_image_reps;
            cs1 = cs.(hrsid).(setid).raw;
            csS = cs.(hrsid).(setid).spatial;
            csM = cs.(hrsid).(setid).spatial_median;

            mainseta = repmat(str2double(mainset{s}(end)), 1, density);
            subseta = repmat(str2double(subset{s}(end)), 1, density);
            expida = repmat(expid(s), 1, density);
            pos1 = 1 : density;

            [col, row] = pos2row_col(density, n_row);
            hours = all_hours(hrs)*ones(1, density);

            for ii = 0 : n_plate - 1
                pos = pos1 + (density * ii);
                plate1 = repmat(ii + 1, 1, density);

                A = [pos; plate1; col; row; mainseta; subseta; hours; CS(1+3*ii, :); ...
                    CS(2+3*ii, :); CS(3+3*ii, :); cs1(1+ii, :); csS(1+ii, :); csM(1+ii, :)];

                values = cell(1, length(pos));
                for uu = 1 : size(pos, 2)
                    values{uu} = sprintf('(%d,%d,%d,%d,%d,%d,%d,%f,%f,%f),', ...
                        [pos(uu),pos(uu)+id,expida(uu),hours(uu),CS(1+3*ii, uu),CS(2+3*ii, uu), ...
                        CS(3+3*ii, uu),cs1(1+ii, uu),csS(1+ii, uu),csM(1+ii, uu)]);
                end
                values = regexprep(values, 'NaN', 'NULL');
                values{end}(end) = '';
                ins_query = sprintf('insert into %s (pos, pid, exp_id, hours, replicate1, replicate2, replicate3, average, csS, csM) values ', foo_table);
                exec(conn, [ins_query, values{:}]);

                if ii == 0 && s == 1 && hrs == 1
                    fid = fopen(textfile, 'w');
                    fprintf(fid, '%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\n', A);
                else
                    fid = fopen(textfile, 'a');
                    fprintf(fid, '%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\t%u\n', A);
                end
            end
            toc;
        end
    end
    fclose(fid);
    clearvars cs;
end
