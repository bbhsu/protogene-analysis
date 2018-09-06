% zero_data_from_source - set values to zero from list of positions
% Brian Hsu, 2015

function zero_ind = zero_data_from_source(imagedir, conn)

    % Load colony information
    cs_replicates = load_colony_sizes(imagedir);
    n_plate = size(cs_replicates, 1) / 3;

    % Average data across three image replicates
    cs_average = squeeze(mean(reshape(cs_replicates, [3, n_plate, 1536]),1));

    [row, col] = pos2row_col(1536, 32);

    if n_plate == 4
        db = 'KO_pos2coor_new2';
    elseif n_plate == 5
        db = 'KO_pos2coor_old';
    end

    map_data = mysql_query(conn, ['select * from ' db ' order by position']);
    cell_map = cell(size(map_data.position, 1), 1);
    for ii = 1 : size(cell_map, 1)
        cell_map{ii} = sprintf('%d,%d,%d', map_data.x1536plate(ii), map_data.x1536row(ii), map_data.x1536col(ii));
    end

    ind = 1;
    for ii = 1 : n_plate
        for pos = 1 : size(row, 2)
            if cs_average(ii, pos) < 10
                zero_map{ind, 1} = sprintf('%d,%d,%d', ii, row(pos), col(pos));
                ind = ind + 1;
            end
        end
    end
    zero_ind = find(ismember(cell_map, zero_map));
end
