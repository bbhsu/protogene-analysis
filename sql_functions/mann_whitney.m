%% mann_whitney
% Perform Mann Whitney U test between target strains and control strains.
% Brian Hsu, 2015

function data = mann_whitney(conn, table)

    %Set preferences with setdbprefs.
    setdbprefs('DataReturnFormat', 'structure');
    setdbprefs('NullNumberRead', 'NaN');
    setdbprefs('NullStringRead', 'null');

    curs = exec(conn, ['SELECT distinct exp_id from ' table]);
  	curs = fetch(curs);
  	close(curs);
  	query1 = curs.Data;
  	all_exp = query1.exp_id;

    for jj = 1 : size(all_exp, 1)
        tic;

        curs = exec(conn, ['SELECT distinct orf_name from ' table ' where exp_id = ' num2str(all_exp(jj))]);
        curs = fetch(curs);
        close(curs);
        orfs = curs.Data;
        if sum(ismember(orfs.orf_name, 'BF_control')) > 0
            cont.name = 'BF_control';
        elseif sum(ismember(orfs.orf_name, 'KO_control')) > 0
            cont.name = 'KO_control';
        end

        %Read data from database.
        set_query = sprintf(['SELECT orf_name'...
        ' ,	exp_id'...
        ' ,	hours'...
        ' ,	fitness'...
        ' FROM ' table ...
        ' WHERE 	orf_name IS NOT NULL'...
        ' AND 	orf_name != "null" ' ...
        ' AND 	exp_id =  ' num2str(all_exp(jj)) ...
        ' ORDER BY 	orf_name ASC, hours ASC ']);

        curs = exec(conn, set_query);
        curs = fetch(curs);
        close(curs);

        %Assign data to output variable
        query3 = curs.Data;

        cont.posy = find(strcmpi(query3.orf_name(:, 1), cont.name)==1);
        cont.yield = query3.fitness(cont.posy, 1)';

        expid = sprintf('exp%d', all_exp(jj));
        inc.t = 1;
        inc.tt = 1;

        for ii = 1 : (size(query3.orf_name, 1))-1
            if(strcmpi(query3.orf_name{ii, 1},query3.orf_name{ii+1, 1})==1)
                temp(1, inc.t) = query3.fitness(ii, 1);
                inc.t=inc.t+1;
                if (ii == size(query3.orf_name, 1)-1)
                    temp(1, inc.t) = query3.fitness(ii+1, 1);
                    if(sum(isnan(temp))==length(temp))
                        data.(expid).p(inc.tt, 1) = NaN;
                        data.(expid).stat(inc.tt, 1) = NaN;
                    else
                        [p, h, stats] = ranksum(temp, cont.yield, 0.05, 'tail', 'both', 'method', 'approximate');
                        data.(expid).p(inc.tt, 1) = p;
                        data.(expid).stat(inc.tt, 1) = stats.zval;
                    end
                    data.(expid).orf_name{inc.tt, 1} = query3.orf_name{ii, 1};
                    data.(expid).exp_id(inc.tt, 1) = all_exp(jj);
                    data.(expid).hours(inc.tt, 1) = query3.hours(ii);
                    inc.tt=inc.tt+1;
                end
            else
                temp(1, inc.t) = query3.fitness(ii, 1);
                if(sum(isnan(temp))==length(temp))
                    data.(expid).p(inc.tt, 1) = NaN;
                    data.(expid).stat(inc.tt, 1) = NaN;
                else
                    [p, h, stats] = ranksum(temp, cont.yield, 0.05, 'tail', 'both', 'method', 'approximate');
                    data.(expid).p(inc.tt, 1) = p;
                    data.(expid).stat(inc.tt, 1) = stats.zval;
                end
                data.(expid).orf_name{inc.tt, 1} = query3.orf_name{ii, 1};
                data.(expid).exp_id(inc.tt, 1) = all_exp(jj);
                data.(expid).hours(inc.tt, 1) = query3.hours(ii);
                clear temp;
                inc.t=1;
                inc.tt=inc.tt+1;
            end
        end
        toc;
    end
end
