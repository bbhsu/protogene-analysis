%% calculate_growth_rate
% Calculate the growth rate using the spatially corrected colony size and difference between time points.
% Brian Hsu, 2015

function data = calculate_growth_rate(conn, table)

    curs = exec(conn, ['SELECT distinct exp_id, pid from ' table]);
    curs = fetch(curs);
    close(curs);
    exp_data = curs.Data;
    all_exp = unique(exp_data.exp_id);
    all_pid = unique(exp_data.pid);

    for jj = 1 : size(all_exp, 1)
        tic;
        SZ = length(all_pid);

        %Read data from database.
        set_query = sprintf(['SELECT pos' ...
        ' ,	pid' ...
        ' ,	exp_id' ...
        ' ,	hours' ...
        ' ,	csM' ...
        ' FROM ' table ...
        ' WHERE exp_id = ' num2str(all_exp(jj)) ...
        ' ORDER BY hours ASC, pos ASC']);
        curs = exec(conn, set_query);
        curs = fetch(curs);
        close(curs);
        all_data = curs.Data;

        expid = sprintf('exp%d', all_exp(jj));
        target_hours = sort(unique(all_data.hours));

        for aa = 1 : length(target_hours)
            spatial_id = sprintf('spatial%d', aa);
            temp.(spatial_id) = all_data.csM(((SZ*(aa-1))+1):(SZ*aa));
        end

        data.(expid).pos = zeros(SZ*(length(target_hours)-1), 1);
        data.(expid).pid = zeros(SZ*(length(target_hours)-1), 1);
        data.(expid).exp_id = zeros(SZ*(length(target_hours)-1), 1);
        data.(expid).hours = zeros(SZ*(length(target_hours)-1), 1);
        data.(expid).GR = zeros(SZ*(length(target_hours)-1), 1);
        for ss = 1 : length(target_hours)-1
            ts1 = sprintf('spatial%d', ss);
            ts2 = sprintf('spatial%d', ss+1);

            data.(expid).pos(SZ*(ss-1)+1:SZ*ss) = all_data.pos(SZ*(ss-1)+1:SZ*ss);
            data.(expid).pid(SZ*(ss-1)+1:SZ*ss) = all_data.pid(SZ*(ss-1)+1:SZ*ss);
            data.(expid).exp_id(SZ*(ss-1)+1:SZ*ss) = all_data.exp_id(SZ*(ss-1)+1:SZ*ss);
            data.(expid).hours(SZ*(ss-1)+1:SZ*ss) = all_data.hours(SZ*(ss-1)+1:SZ*ss);
            data.(expid).GR(SZ*(ss-1)+1:SZ*ss) = (temp.(ts2) - temp.(ts1)) / (target_hours(ss+1) - target_hours(ss));
        end
        clear temp;
        toc;
    end
end
