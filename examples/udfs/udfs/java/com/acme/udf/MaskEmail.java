package com.acme.udf;

import org.apache.flink.table.functions.ScalarFunction;

/** mask_email('jane@example.com') = 'j***@example.com'. */
public class MaskEmail extends ScalarFunction {
    public String eval(String email) {
        if (email == null) {
            return null;
        }
        int at = email.indexOf('@');
        if (at <= 0) {
            return "***";
        }
        return email.charAt(0) + "***" + email.substring(at);
    }
}
