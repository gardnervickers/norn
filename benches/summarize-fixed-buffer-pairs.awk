BEGIN {
    FS = "\t"
    OFS = "\t"

    if (result_dir == "" || pairs_path == "" || summary_path == "") {
        fatal("result_dir, pairs_path, and summary_path are required")
    }
}

function fatal(message) {
    print "fixed-buffer summary: " message > "/dev/stderr"
    fatal_error = 1
    exit 2
}

function number(text, value) {
    value = text
    gsub(/,/, "", value)
    return value + 0
}

function median(values, count, sorted, middle) {
    delete sorted
    for (middle = 1; middle <= count; middle++) {
        sorted[middle] = values[middle]
    }
    asort(sorted)
    middle = int(count / 2) + 1
    if (count % 2 == 1) {
        return sorted[middle]
    }
    return (sorted[middle - 1] + sorted[middle]) / 2
}

function choose(n, k, value, i) {
    if (k > n - k) {
        k = n - k
    }
    value = 1
    for (i = 1; i <= k; i++) {
        value = value * (n - k + i) / i
    }
    return value
}

function sign_test_p(positive, negative, n, minority, total, k, p) {
    n = positive + negative
    if (n == 0) {
        return 1
    }
    minority = positive < negative ? positive : negative
    total = 0
    for (k = 0; k <= minority; k++) {
        total += choose(n, k)
    }
    p = 2 * total / (2 ^ n)
    return p > 1 ? 1 : p
}

function key_compare(left, left_value, right, right_value, a, b) {
    split(left, a, SUBSEP)
    split(right, b, SUBSEP)
    if (a[1] != b[1]) {
        return a[1] == "read" ? -1 : 1
    }
    if ((a[2] + 0) != (b[2] + 0)) {
        return (a[2] + 0) - (b[2] + 0)
    }
    return (a[3] + 0) - (b[3] + 0)
}

NR == 1 {
    if ($0 != "direction\tqd\ttrial\tposition\tmode\tlog") {
        fatal("unexpected order.tsv header")
    }
    next
}

{
    direction = $1
    qd = $2 + 0
    trial = $3 + 0
    position = $4 + 0
    mode = $5
    log_name = $6

    if (direction != "read" && direction != "write") {
        fatal("unexpected direction on manifest row " NR ": " direction)
    }
    if (qd <= 0 || trial <= 0 || (position != 1 && position != 2)) {
        fatal("invalid queue depth, trial, or position on manifest row " NR)
    }
    if (mode != "ordinary" && mode != "fixed") {
        fatal("unexpected mode on manifest row " NR ": " mode)
    }

    pair_key = direction SUBSEP qd SUBSEP trial
    mode_key = pair_key SUBSEP mode
    position_key = pair_key SUBSEP position
    if (mode_seen[mode_key] || position_seen[position_key]) {
        fatal("duplicate mode or position for " direction " qd=" qd " trial=" trial)
    }
    mode_seen[mode_key] = 1
    position_seen[position_key] = 1
    position_mode[position_key] = mode

    log_path = result_dir "/" log_name
    expected = "fixed_file_io/mode=" mode "/direction=" direction \
        "/storage=aligned_heap/block=4096/qd=" qd "/ops=16384"
    matches = 0
    saw_expected = 0
    read_status = 0
    while ((read_status = (getline log_line < log_path)) > 0) {
        if (index(log_line, expected) != 0) {
            saw_expected = 1
        }
        if (match(log_line, /bench:[[:space:]]+([0-9,]+) ns\/iter \(\+\/- ([0-9,]+)\)/, fields)) {
            matches++
            estimate[mode_key] = number(fields[1])
            dispersion[mode_key] = number(fields[2])
        }
    }
    close(log_path)
    if (read_status < 0) {
        fatal("could not read " log_path)
    }
    if (!saw_expected) {
        fatal("benchmark name does not match manifest in " log_name)
    }
    if (matches != 1) {
        fatal("expected one benchmark result in " log_name ", found " matches)
    }

    pairs[pair_key] = 1
    group_key = direction SUBSEP qd
    groups[group_key] = 1
    group_manifest_rows[group_key]++
}

END {
    if (fatal_error) {
        exit 2
    }

    print "direction", "qd", "trial", "first_mode", \
        "ordinary_ns", "ordinary_dispersion_ns", "fixed_ns", \
        "fixed_dispersion_ns", "delta_ns", "delta_pct", \
        "ordinary_dispersion_pct", "fixed_dispersion_pct" > pairs_path

    pair_count = asorti(pairs, ordered_pairs, "key_compare")
    for (pair_index = 1; pair_index <= pair_count; pair_index++) {
        pair_key = ordered_pairs[pair_index]
        split(pair_key, key_fields, SUBSEP)
        direction = key_fields[1]
        qd = key_fields[2] + 0
        trial = key_fields[3] + 0
        ordinary_key = pair_key SUBSEP "ordinary"
        fixed_key = pair_key SUBSEP "fixed"
        if (!mode_seen[ordinary_key] || !mode_seen[fixed_key]) {
            fatal("incomplete pair for " direction " qd=" qd " trial=" trial)
        }
        if (!position_seen[pair_key SUBSEP 1] || !position_seen[pair_key SUBSEP 2]) {
            fatal("missing execution position for " direction " qd=" qd " trial=" trial)
        }

        ordinary_ns = estimate[ordinary_key]
        fixed_ns = estimate[fixed_key]
        delta_ns = fixed_ns - ordinary_ns
        delta_pct = 100 * (fixed_ns / ordinary_ns - 1)
        ordinary_dispersion_pct = 100 * dispersion[ordinary_key] / ordinary_ns
        fixed_dispersion_pct = 100 * dispersion[fixed_key] / fixed_ns
        first_mode = position_mode[pair_key SUBSEP 1]

        pair_delta[pair_key] = delta_pct
        pair_first_mode[pair_key] = first_mode
        printf "%s\t%d\t%d\t%s\t%.0f\t%.0f\t%.0f\t%.0f\t%.0f\t%.6f\t%.6f\t%.6f\n", \
            direction, qd, trial, first_mode, ordinary_ns, dispersion[ordinary_key], \
            fixed_ns, dispersion[fixed_key], delta_ns, delta_pct, \
            ordinary_dispersion_pct, fixed_dispersion_pct > pairs_path
    }

    print "direction", "qd", "pairs", "ordinary_median_ms", \
        "fixed_median_ms", "ratio_of_medians_pct", "paired_delta_median_pct", \
        "paired_delta_mad_pct", "paired_delta_min_pct", "paired_delta_max_pct", \
        "fixed_faster_count", "sign_test_p", "ordinary_first_median_pct", \
        "fixed_first_median_pct", "ordinary_dispersion_median_pct", \
        "fixed_dispersion_median_pct" > summary_path

    group_count = asorti(groups, ordered_groups, "key_compare")
    for (group_index = 1; group_index <= group_count; group_index++) {
        group_key = ordered_groups[group_index]
        split(group_key, key_fields, SUBSEP)
        direction = key_fields[1]
        qd = key_fields[2] + 0
        expected_pairs = group_manifest_rows[group_key] / 2

        delete ordinary_values
        delete fixed_values
        delete delta_values
        delete ordinary_dispersion_values
        delete fixed_dispersion_values
        delete ordinary_first_values
        delete fixed_first_values
        values = 0
        ordinary_first_count = 0
        fixed_first_count = 0
        fixed_faster = 0
        fixed_slower = 0
        delta_min = 0
        delta_max = 0

        for (pair_index = 1; pair_index <= pair_count; pair_index++) {
            pair_key = ordered_pairs[pair_index]
            split(pair_key, pair_fields, SUBSEP)
            if (pair_fields[1] != direction || (pair_fields[2] + 0) != qd) {
                continue
            }
            ordinary_key = pair_key SUBSEP "ordinary"
            fixed_key = pair_key SUBSEP "fixed"
            values++
            ordinary_values[values] = estimate[ordinary_key]
            fixed_values[values] = estimate[fixed_key]
            delta_values[values] = pair_delta[pair_key]
            ordinary_dispersion_values[values] = 100 * dispersion[ordinary_key] / estimate[ordinary_key]
            fixed_dispersion_values[values] = 100 * dispersion[fixed_key] / estimate[fixed_key]
            if (pair_delta[pair_key] < 0) {
                fixed_faster++
            } else if (pair_delta[pair_key] > 0) {
                fixed_slower++
            }
            if (values == 1 || pair_delta[pair_key] < delta_min) {
                delta_min = pair_delta[pair_key]
            }
            if (values == 1 || pair_delta[pair_key] > delta_max) {
                delta_max = pair_delta[pair_key]
            }
            if (pair_first_mode[pair_key] == "ordinary") {
                ordinary_first_values[++ordinary_first_count] = pair_delta[pair_key]
            } else {
                fixed_first_values[++fixed_first_count] = pair_delta[pair_key]
            }
        }
        if (values != expected_pairs || values < 2 || ordinary_first_count == 0 || fixed_first_count == 0) {
            fatal("incomplete summary group for " direction " qd=" qd)
        }

        ordinary_median = median(ordinary_values, values)
        fixed_median = median(fixed_values, values)
        delta_median = median(delta_values, values)
        delete deviations
        for (value_index = 1; value_index <= values; value_index++) {
            deviation = delta_values[value_index] - delta_median
            deviations[value_index] = deviation < 0 ? -deviation : deviation
        }
        delta_mad = median(deviations, values)
        ratio_of_medians = 100 * (fixed_median / ordinary_median - 1)
        sign_p = sign_test_p(fixed_slower, fixed_faster)

        printf "%s\t%d\t%d\t%.6f\t%.6f\t%.6f\t%.6f\t%.6f\t%.6f\t%.6f\t%d\t%.6f\t%.6f\t%.6f\t%.6f\t%.6f\n", \
            direction, qd, values, ordinary_median / 1000000, fixed_median / 1000000, \
            ratio_of_medians, delta_median, delta_mad, delta_min, delta_max, \
            fixed_faster, sign_p, median(ordinary_first_values, ordinary_first_count), \
            median(fixed_first_values, fixed_first_count), \
            median(ordinary_dispersion_values, values), \
            median(fixed_dispersion_values, values) > summary_path
    }

    close(pairs_path)
    close(summary_path)
}
