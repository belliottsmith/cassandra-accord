package accord.utils;

public interface TriFunction<P1, P2, P3, O>
{
    O apply(P1 p1, P2 p2, P3 p3);
}
