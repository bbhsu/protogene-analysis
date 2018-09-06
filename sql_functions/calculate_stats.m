%% calculate_stats
% Calculate mean and median fitness for all strains.
% Brian Hsu, 2015

function data = calculate_stats(conn, table)

    curs = exec(conn, ['SELECT distinct exp_id from ' table]);
  	curs = fetch(curs);
  	close(curs);
  	query1 = curs.Data;
  	all_exp = query1.exp_id;

    for jj = 1 : size(all_exp, 1)
        tic;

        inc.tt=1;

        %Read data from database.
        curs = exec(conn, ['SELECT orf_name'...
        ' ,	exp_id'...
        ' ,	hours'...
        ' ,	fitness'...
        ' FROM 	' table ...
        ' WHERE orf_name IS NOT NULL'...
        ' AND orf_name != "null" ' ...
        ' AND exp_id =  ' num2str(all_exp(jj)) ...
        ' ORDER BY orf_name ASC, hours ASC']);

        curs = fetch(curs);
        close(curs);

        %Assign data to output variable
        query2 = curs.Data;

        expid = sprintf('exp%d', all_exp(jj));
        inc.t=1;
        for ii = 1 : (size(query2.orf_name, 1))-1
            if(strcmpi(query2.orf_name{ii, 1},query2.orf_name{ii+1, 1})==1)
                temp(1, inc.t) = query2.fitness(ii, 1);
                inc.t=inc.t+1;
                if (ii == size(query2.orf_name, 1)-1)
                    temp(1, inc.t) = query2.fitness(ii+1, 1);
                    data.(expid).median(inc.tt, 1) = nanmedian(temp);
                    data.(expid).mean(inc.tt, 1) = nanmean(temp);
                    data.(expid).orf_name{inc.tt, 1} = query2.orf_name{ii, 1};
                    data.(expid).exp_id(inc.tt, 1) = all_exp(jj);
                    data.(expid).hours(inc.tt, 1) = query2.hours(ii, 1);
                    inc.tt=inc.tt+1;
                end
            else
                temp(1, inc.t) = query2.fitness(ii, 1);
                data.(expid).median(inc.tt, 1) = nanmedian(temp);
                data.(expid).mean(inc.tt, 1) = nanmean(temp);
                data.(expid).orf_name{inc.tt, 1} = query2.orf_name{ii, 1};
                data.(expid).exp_id(inc.tt, 1) = all_exp(jj);
                data.(expid).hours(inc.tt, 1) = query2.hours(ii, 1);
                clear temp;
                inc.t=1;
                inc.tt=inc.tt+1;
            end
        end
        toc;
    end
end
