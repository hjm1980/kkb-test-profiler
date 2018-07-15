package kr.jm.test.kkb.profiler.repository.profile;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@ToString
@Getter
public class AbstractProfile {
    private int customer_number;
}
