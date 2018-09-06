%% sql_query
% Wrapper function to execute SQL query.
% Brian Hsu, 2015

function data = sql_query(conn, query)

    %Set preferences with setdbprefs.
    setdbprefs('DataReturnFormat', 'structure');
    setdbprefs('NullNumberRead', 'NaN');
    setdbprefs('NullStringRead', 'null');

    curs = exec(conn, query);

    curs = fetch(curs);
    close(curs);

    data = curs.Data;
end
