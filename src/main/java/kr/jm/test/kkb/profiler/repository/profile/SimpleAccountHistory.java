package kr.jm.test.kkb.profiler.repository.profile;

import lombok.*;

@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@ToString
@Getter
public class SimpleAccountHistory {
    private long amount;
    private String datetime;
}
