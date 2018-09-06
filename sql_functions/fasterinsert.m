%% fasterinsert
% Insert data structure into SQL table via SQL syntax.
% Faster runtime than fastinsert.
% Brian Hsu, 2015

function fasterinsert(conn, data, col_names, col_format, foo_table)

    insert_data = cell(1, length(col_names));
    tic;
    values = cell(1, length(data.(col_names{1})));
    for ii = 1 : length(values)
        for jj = 1 : length(col_names)
            if iscell(data.(col_names{jj}))
                insert_data{jj} = data.(col_names{jj}){ii};
            else
                insert_data{jj} = data.(col_names{jj})(ii);
            end
        end
        values{ii} = sprintf(strcat('(', col_format, '),'), insert_data{:});
    end

    values = regexprep(values, 'NaN', 'NULL');
    values{end}(end) = '';

    str_format = repmat('%s, ', 1, length(col_names));
    str_format(end-1:end) = '';

    ins_query = sprintf(strcat('insert into %s (', str_format, ') values '), foo_table, col_names{:});
    exec(conn, [ins_query, values{:}]);
    toc;
end
