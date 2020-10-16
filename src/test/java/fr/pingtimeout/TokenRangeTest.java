package fr.pingtimeout;

import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenRangeTest
{
    @Test
    void should_check_inputs()
    {
        assertThatThrownBy(() -> new TokenRange(5, -5))
            .hasMessage("Lower bound 5 cannot be greater than upper bound -5")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void should_allow_range_with_single_token()
    {
        TokenRange tokenRange = new TokenRange(0, 0);
        String stringRepresentation = tokenRange.toString();
        assertThat(stringRepresentation).isEqualTo("[0;0]");
    }

    @Test
    void should_be_readable()
    {
        TokenRange tokenRange = new TokenRange(-10, 10);
        String stringRepresentation = tokenRange.toString();
        assertThat(stringRepresentation).isEqualTo("[-10;10]");
    }

    @Test
    void should_tell_if_it_contains_a_token()
    {
        TokenRange tokenRange = new TokenRange(-10, 10);
        assertThat(tokenRange.contains(5)).isTrue();
    }

    @Test
    void should_contain_its_bounds()
    {
        TokenRange tokenRange = new TokenRange(-10, 10);
        assertThat(tokenRange.contains(-11)).isFalse();
        assertThat(tokenRange.contains(-10)).isTrue();
        assertThat(tokenRange.contains(10)).isTrue();
        assertThat(tokenRange.contains(11)).isFalse();
    }

    @Test
    void should_intersect_with_its_bound_plus_or_minus_one()
    {
        TokenRange tokenRange = new TokenRange(-10, 10);
        assertThat(tokenRange.canIntersectWith(-12)).isFalse();
        assertThat(tokenRange.canIntersectWith(-11)).isTrue();
        assertThat(tokenRange.canIntersectWith(-10)).isTrue();
        assertThat(tokenRange.canIntersectWith(10)).isTrue();
        assertThat(tokenRange.canIntersectWith(11)).isTrue();
        assertThat(tokenRange.canIntersectWith(12)).isFalse();
    }

    @Test
    void should_detect_intersection_at_lower_bound()
    {
        TokenRange tokenRangeA = new TokenRange(5, 15);
        TokenRange tokenRangeB = new TokenRange(10, 20);
        assertThat(tokenRangeA.intersectsWith(tokenRangeB)).isTrue();
    }

    @Test
    void should_detect_intersection_at_upper_bound()
    {
        TokenRange tokenRangeA = new TokenRange(10, 20);
        TokenRange tokenRangeB = new TokenRange(5, 15);
        assertThat(tokenRangeA.intersectsWith(tokenRangeB)).isTrue();
    }

    @Test
    void should_be_built_from_merging_two_contiguous_ranges()
    {
        TokenRange tokenRangeA = new TokenRange(0, 10);
        TokenRange tokenRangeB = new TokenRange(11, 20);
        assertThat(tokenRangeA.mergeWith(tokenRangeB).getLowerBound()).isEqualTo(tokenRangeA.getLowerBound());
        assertThat(tokenRangeA.mergeWith(tokenRangeB).getUpperBound()).isEqualTo(tokenRangeB.getUpperBound());
    }

    @Test
    void should_be_built_from_merging_two_contiguous_ranges_without_order_preference()
    {
        TokenRange tokenRangeA = new TokenRange(0, 10);
        TokenRange tokenRangeB = new TokenRange(9, 20);
        TokenRange merge = tokenRangeB.mergeWith(tokenRangeA);
        assertThat(merge.getLowerBound()).isEqualTo(tokenRangeA.getLowerBound());
        assertThat(merge.getUpperBound()).isEqualTo(tokenRangeB.getUpperBound());
    }

    @Test
    void should_be_built_from_merging_two_overlapping_ranges_without_order_preference()
    {
        TokenRange tokenRangeA = new TokenRange(0, 10);
        TokenRange tokenRangeB = new TokenRange(5, 20);
        TokenRange merge = tokenRangeB.mergeWith(tokenRangeA);
        assertThat(merge.getLowerBound()).isEqualTo(tokenRangeA.getLowerBound());
        assertThat(merge.getUpperBound()).isEqualTo(tokenRangeB.getUpperBound());
    }

    @Test
    void should_throw_when_merging_token_ranges_that_are_not_contiguous()
    {
        TokenRange tokenRangeA = new TokenRange(-10, -1);
        TokenRange tokenRangeB = new TokenRange(1, 10);
        assertThatThrownBy(() -> tokenRangeA.mergeWith(tokenRangeB))
            .hasMessage("Cannot merge ranges [-10;-1] and [1;10] as they are not contiguous")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void should_sort_by_token_range()
    {
        Iterator<TokenRange> iterator = new TreeSet<>(
            Arrays.asList(
                new TokenRange(400, 430),
                new TokenRange(-5, 30),
                new TokenRange(-5, 5),
                new TokenRange(-10, -1)))
            .iterator();
        assertThat(iterator.next()).isEqualTo(new TokenRange(-10, -1));
        assertThat(iterator.next()).isEqualTo(new TokenRange(-5, 5));
        assertThat(iterator.next()).isEqualTo(new TokenRange(-5, 30));
        assertThat(iterator.next()).isEqualTo(new TokenRange(400, 430));
    }
}