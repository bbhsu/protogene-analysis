% pos2row_col - convert position to row/column
% Accepts plate density number.
% Brian Hsu, 2015

function [row, col] = pos2row_col(density, n_row)

    col = zeros(1, density);
    row = zeros(1, density);
    for p = 1 : density
        if (rem(p, n_row) == 0)
            col(1, p) = floor(p/n_row);
            row(1, p) = n_row;
        else
            col(1, p) = floor(p/n_row) + 1;
            row(1, p) = rem(p, n_row);
        end
    end
end
