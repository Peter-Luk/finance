factorial(A,B) :-
    A > 0,
    C is A - 1,
    factorial(C,D),
    B is A * D.
factorial(0,1).
move(1,X,Y,_) :-
    write('Move top disk from '),
    write(X),
    write(' to '),
    write(Y),
    nl.
move(N,X,Y,Z):-
    N>1,
    M is N-1,
    move(M,X,Z,Y),
    move(1,X,Y,_),
    move(M,Z,Y,X).
