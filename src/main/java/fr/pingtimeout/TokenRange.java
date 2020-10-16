package fr.pingtimeout;

import java.util.Comparator;
import java.util.Objects;

public class TokenRange implements Comparable<TokenRange>
{
    public static final TokenRange FULL_TOKEN_RANGE = new TokenRange(Long.MIN_VALUE, Long.MAX_VALUE);
    private static final Comparator<TokenRange> COMPARATOR = Comparator
        .comparing(TokenRange::getLowerBound)
        .thenComparing(TokenRange::getUpperBound);

    private final long lowerBound;
    private final long upperBound;

    public TokenRange(long lowerBound, long upperBound)
    {
        if (lowerBound > upperBound)
        {
            throw new IllegalArgumentException(
                "Lower bound " + lowerBound + " cannot be greater than upper bound " + upperBound);
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    long getLowerBound()
    {
        return lowerBound;
    }

    long getUpperBound()
    {
        return upperBound;
    }

    @Override
    public String toString()
    {
        return "[" + lowerBound + ";" + upperBound + ']';
    }

    boolean contains(long token)
    {
        return token >= lowerBound && token <= upperBound;
    }

    boolean intersectsWith(TokenRange that)
    {
        return this.canIntersectWith(that.lowerBound) || this.canIntersectWith(that.upperBound);
    }

    boolean canIntersectWith(long bound)
    {
        return this.contains(bound - 1) ||
            this.contains(bound) ||
            this.contains(bound + 1);
    }

    TokenRange mergeWith(TokenRange that)
    {
        if (this.intersectsWith(that))
        {
            long newLowerBound = this.lowerBound < that.lowerBound ? this.lowerBound : that.lowerBound;
            long newUpperBound = this.upperBound > that.upperBound ? this.upperBound : that.upperBound;
            return new TokenRange(newLowerBound, newUpperBound);
        }
        else
        {
            throw new IllegalArgumentException(
                "Cannot merge ranges " + this.toString() + " and " + that.toString() + " as they are not contiguous");
        }
    }

    TokenRange withUpperBound(long newUpperBound) {
        return new TokenRange(this.lowerBound, newUpperBound);
    }

    TokenRange withLowerBound(long newLowerBound)
    {
        return new TokenRange(newLowerBound, this.upperBound);
    }

    @Override
    public int compareTo(TokenRange that)
    {
        return COMPARATOR.compare(this, that);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        TokenRange that = (TokenRange) o;
        return lowerBound == that.lowerBound &&
            upperBound == that.upperBound;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowerBound, upperBound);
    }
}
