%% calculate_growth_fitness
% Calculate the fitness as defined by the growth rate.
% Brian Hsu, 2015

function data = calculate_growth_fitness(conn, table)

    curs = exec(conn, ['SELECT distinct pid, exp_id from ' table]);
  	curs = fetch(curs);
  	close(curs);
  	exp_data = curs.Data;
  	all_exp = unique(exp_data.exp_id);
    all_pid = unique(exp_data.pid);

    pid = all_pid(1);
    curs = exec(conn, ['select * from POS2ORF_NAME_new where pid between ' num2str(pid-1) ' and ' num2str(pid-1+100000) ' order by pid']);
    curs = fetch(curs);
    close(curs);
    pos_data = curs.Data;

    all_orfs = unique(pos_data.orf_name);

    for ii = 1 : length(all_exp)
        tic;
        curs = exec(conn, ['SELECT pos'...
            ' ,	pid'...
            ' ,	exp_id'...
            ' ,	hours'...
            ' ,	GR'...
            ' FROM ' table ...
            ' WHERE exp_id = ' num2str(all_exp(ii)) ...
            ' ORDER BY 	hours ASC, pid ASC']);

        curs = fetch(curs);
        close(curs);
        all_data = curs.Data;

        SZ = length(all_pid);
        all_hours = unique(all_data.hours);

        expid = sprintf('exp%d', all_exp(ii));

        data.(expid).orf_name = pos_data.orf_name;
        data.(expid).pid = pos_data.pid;
        data.(expid).exp_id = all_data.exp_id(1:SZ, 1);
        data.(expid).hours = NaN(SZ, 1);
        data.(expid).is_orf = pos_data.is_orf;
        data.(expid).fitness = NaN(SZ, 1);

        for jj = 1 : length(all_orfs)
            gr_max = -100;
            orf_ind = pos_data.pid((strcmpi(pos_data.orf_name, all_orfs(jj))==1));
            ind = find(ismember(all_data.pid, orf_ind));
            for hh = 1 : length(all_hours)
                ind_hr = ind(all_data.hours(ind)==all_hours(hh));
                gr_avg = nanmedian(all_data.GR(ind_hr));
                if gr_avg < 0.8 * gr_max || (hh > 3 && gr_avg < 5)
                    break;
                elseif gr_avg > gr_max
                    pos = unique(all_data.pos(ind_hr));
                    data.(expid).fitness(pos, 1) = all_data.GR(ind_hr);
                    data.(expid).hours(pos, 1) = all_data.hours(ind_hr);
                    gr_max = gr_avg;
                end
            end
        end
        toc;
    end
end
