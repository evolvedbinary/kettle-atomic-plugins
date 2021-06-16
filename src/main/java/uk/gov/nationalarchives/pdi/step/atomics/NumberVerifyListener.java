/**
 * The MIT License
 * Copyright © 2021 The National Archives
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package uk.gov.nationalarchives.pdi.step.atomics;

import com.evolvedbinary.j8fu.function.ConsumerE;
import org.eclipse.swt.events.VerifyEvent;
import org.eclipse.swt.events.VerifyListener;
import org.eclipse.swt.widgets.Text;

import java.util.function.Function;

public class NumberVerifyListener implements VerifyListener {
    private final Function<VerifyEvent, String> fnGetOldString;
    private final ConsumerE<String, NumberFormatException> parseChecker;

    NumberVerifyListener(final Function<VerifyEvent, String> fnGetOldString, final ConsumerE<String, NumberFormatException> parseChecker) {
        this.fnGetOldString = fnGetOldString;
        this.parseChecker = parseChecker;
    }

    public NumberVerifyListener(final ConsumerE<String, NumberFormatException> parseChecker) {
        this(verifyEvent -> {
            final Text text = (Text) verifyEvent.getSource();
            final String oldString = text.getText();
            return oldString;
        }, parseChecker);
    }

    @Override
    public void verifyText(final VerifyEvent verifyEvent) {
        // ONLY allow integers!

        // get old string and create new string by using the VerifyEvent.text
        final String oldString = fnGetOldString.apply(verifyEvent);

        final String newString = oldString.substring(0, verifyEvent.start) + verifyEvent.text + oldString.substring(verifyEvent.end);

        boolean isValid = false;
        try {
            parseChecker.accept(newString);
            isValid = true;
        } catch(final NumberFormatException ex) {
            isValid = false;
        }

        if(!isValid) {
            verifyEvent.doit = false;
        }
    }
}
