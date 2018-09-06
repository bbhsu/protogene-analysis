%% calculate_percentile
% Calculate the 5th and 95th percentile of fitnes of the control strain.
% Brian Hsu, 2015

function data1 = calculate_percentile(conn, table)

    curs = exec(conn, ['SELECT distinct exp_id from ' table]);
  	curs = fetch(curs);
  	close(curs);
  	exp_data = curs.Data;
  	all_exp = exp_data.exp_id;

    for jj = 1 : size(all_exp, 1)

        tic;
        expid = sprintf('exp%d', all_exp(jj));
        data1.(expid).exp_id = [];
        data1.(expid).hours = [];
        data1.(expid).perc5 = [];
        data1.(expid).perc95 = [];

        curs = exec(conn, ['SELECT distinct orf_name from ' table ' where exp_id = ' num2str(all_exp(jj))]);
        curs = fetch(curs);
        close(curs);
        orfs = curs.Data;

        if sum(ismember(orfs.orf_name, 'BF_control')) > 0
            cont = -2;
        elseif sum(ismember(orfs.orf_name, 'KO_control')) > 0
            cont = -1;
        end

        set_query = sprintf(['SELECT orf_name' ...
        ' ,	exp_id' ...
        ' ,	hours' ...
        ' ,	is_orf' ...
        ' ,	fitness' ...
        ' FROM ' table ...
        ' WHERE 	orf_name IS NOT NULL' ...
        ' AND 	is_orf = ' num2str(cont) ...
        ' AND 	exp_id =  ' num2str(all_exp(jj))]);

        curs = exec(conn, set_query);

        curs = fetch(curs);
        close(curs);

        %Assign data to output variable
        fitness_data = curs.Data;

        data.(expid).exp_id = all_exp(jj);
        data.(expid).hours = fitness_data.hours(1);
        data.(expid).perc5 = prctile(fitness_data.fitness, 5);
        data.(expid).perc95 = prctile(fitness_data.fitness, 95);

        data1.(expid).exp_id = [data1.(expid).exp_id; data.(expid).exp_id];
        data1.(expid).hours= [data1.(expid).hours; data.(expid).hours];
        data1.(expid).perc5 = [data1.(expid).perc5; data.(expid).perc5];
        data1.(expid).perc95 = [data1.(expid).perc95; data.(expid).perc95];
        toc;
    end

end
