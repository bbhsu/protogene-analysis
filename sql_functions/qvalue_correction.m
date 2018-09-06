%% qvalue_correction
% Perform Q-value correction for all generateed p-values.
% Brian Hsu, 2015

function data1 = qvalue_correction(conn, table)

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

       %Read data from database.
        set_query = sprintf(['SELECT orf_name'...
        ' ,	exp_id'...
        ' ,	hours'...
        ' ,	p'...
        ' ,	stat'...
        ' FROM  ' table ...
        ' WHERE 	orf_name IS NOT NULL'...
        ' AND 	orf_name != "null" ' ...
        ' AND 	exp_id =  ' num2str(all_exp(jj))]);

        curs = exec(conn, set_query);
        curs = fetch(curs);
        close(curs);
        query2 = curs.Data;

        expid = sprintf('exp%d', all_exp(jj));

        data1.(expid).orf_name = [];
        data1.(expid).exp_id = [];
        data1.(expid).hours = [];
        data1.(expid).q = [];
        data1.(expid).p = [];
        data1.(expid).stat = [];

        if (strcmpi(query2, 'No Data')~=1)
            [~, qy] = mafdr(query2.p);
            [pvals.y, pvals.ypos] = sort(qy);
            pp.y = size(pvals.y);

            data.(expid).orf_name = query2.orf_name(pvals.ypos(1:pp.y));
            data.(expid).exp_id(1:pp.y, 1) = all_exp(jj);
            data.(expid).hours(1:pp.y, 1) = query2.hours(pvals.ypos(1:pp.y));
            data.(expid).q = pvals.y(1:pp.y);
            data.(expid).p = query2.p(pvals.ypos(1:pp.y));
            data.(expid).stat = query2.stat(pvals.ypos(1:pp.y));

            data1.(expid).orf_name = [data1.(expid).orf_name; data.(expid).orf_name];
            data1.(expid).exp_id = [data1.(expid).exp_id; data.(expid).exp_id];
            data1.(expid).hours= [data1.(expid).hours; data.(expid).hours];
            data1.(expid).q = [data1.(expid).q; data.(expid).q];
            data1.(expid).p = [data1.(expid).p; data.(expid).p];
            data1.(expid).stat = [data1.(expid).stat; data.(expid).stat];
        end
        toc;
    end
end
