%% calculate_fitness
% Query database for normalized colony size (csS) across all time points.
% Find the time point at which the difference between median raw colony size
% of the current time point and previous time point is less than 5%.
% Set the fitness as the normalized colony size at this time point.
% Brian Hsu, 2015

function data = calculate_fitness(conn, table)

    % Get experiment IDs
    curs = exec(conn, ['SELECT distinct exp_id, pid, hours from ' table]);
  	curs = fetch(curs);
  	close(curs);
  	exp_data = curs.Data;
  	all_exp = sort(unique(exp_data.exp_id));
    all_pid = unique(exp_data.pid);
    all_hours = unique(exp_data.hours);

    pid = all_pid(1);
    curs = exec(conn, ['select * from POS2ORF_NAME_new where pid between ' num2str(pid-1) ' and ' num2str(pid-1+100000) ' order by pid']);
    curs = fetch(curs);
    close(curs);
    pos_data = curs.Data;

    all_orfs = unique(pos_data.orf_name);

    for ii = 1 : length(all_exp)
        tic;

        curs = exec(conn, ['SELECT  pos' ...
            ' , pid' ...
            ' ,	exp_id' ...
            ' ,	hours' ...
            ' ,	average' ...
            ' ,	csS' ...
            ' FROM ' table ...
            ' WHERE exp_id = ' num2str(all_exp(ii)) ...
            ' ORDER BY 	hours ASC, pid ASC']);

        curs = fetch(curs);
        close(curs);
        all_data = curs.Data;
        SZ = length(all_pid);

        expid = sprintf('exp%d', all_exp(ii));

        data.(expid).orf_name = pos_data.orf_name;
        data.(expid).pid = pos_data.pid;
        data.(expid).exp_id = all_data.exp_id(1:SZ, 1);
        data.(expid).hours = NaN(SZ, 1);
        data.(expid).is_orf = pos_data.is_orf;
        data.(expid).average = NaN(SZ, 1);
        data.(expid).fitness = NaN(SZ, 1);

        ind = find(ismember(all_data.pid, pos_data.pid(~cellfun(@isempty, strfind(pos_data.orf_name, 'control')))));
        cs_max = -1;
        for hh = 1 : length(all_hours)
            ind_hr = ind(all_data.hours(ind)==all_hours(hh));
            cs_avg = nanmedian(all_data.average(ind_hr));
            if cs_avg < 0.95 * cs_max
                break;
            elseif cs_avg > 1.05 * cs_max
                fitness_hour = all_hours(hh);
                pos = all_data.pos(ind_hr);
                data.(expid).average(pos, 1) = all_data.average(ind_hr);
                data.(expid).fitness(pos, 1) = all_data.csS(ind_hr);
                data.(expid).hours(pos, 1) = all_data.hours(ind_hr);
                cs_max = cs_avg;
            end
        end

        for jj = 1 : length(all_orfs)
            ind = find(ismember(all_data.pid, pos_data.pid(strcmpi(pos_data.orf_name, all_orfs(jj))==1)));
            ind_hr = ind(all_data.hours(ind)==fitness_hour);
            pos = all_data.pos(ind_hr);
            data.(expid).average(pos, 1) = all_data.average(ind_hr);
            data.(expid).fitness(pos, 1) = all_data.csS(ind_hr);
            data.(expid).hours(pos, 1) = all_data.hours(ind_hr);
        end
        toc;
    end
end
