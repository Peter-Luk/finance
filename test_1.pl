factorial(0,1).
factorial(A,B) :-
    A > 0,
    C is A - 1,
    factorial(C,D),
    B is A * D.
list_length(Xs,L) :- list_length(Xs,0,L).
list_length( []     , L , L ).
list_length( [_|Xs] , T , L ) :-
    T1 is T+1,
    list_length(Xs,T1,L).
