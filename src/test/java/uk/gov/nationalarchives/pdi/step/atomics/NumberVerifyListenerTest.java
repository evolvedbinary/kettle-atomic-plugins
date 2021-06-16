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
import org.eclipse.swt.widgets.Widget;
import org.junit.jupiter.api.Test;

import org.eclipse.swt.widgets.Event;

import static org.mockito.Mockito.mock;
import static org.junit.jupiter.api.Assertions.*;

public class NumberVerifyListenerTest {

    @Test
    public void verifyTextOk() {
        final CapturingParseOk parseChecker = new CapturingParseOk();
        final NumberVerifyListener numberVerifyListener = new NumberVerifyListener(verifyEvent -> "1234", parseChecker);

        final Event event = new Event();
        event.widget = mock(Widget.class);
        final VerifyEvent verifyEvent = new VerifyEvent(event);
        verifyEvent.text = "678";

        numberVerifyListener.verifyText(verifyEvent);

        assertTrue(verifyEvent.doit);
        assertEquals("6781234", parseChecker.captured);
    }

    @Test
    public void verifyTextFail() {
        final CapturingParseFail parseChecker = new CapturingParseFail();
        final NumberVerifyListener numberVerifyListener = new NumberVerifyListener(verifyEvent -> "1234", parseChecker);

        final Event event = new Event();
        event.widget = mock(Widget.class);
        final VerifyEvent verifyEvent = new VerifyEvent(event);
        verifyEvent.text = "678";

        numberVerifyListener.verifyText(verifyEvent);

        assertFalse(verifyEvent.doit);
        assertEquals("6781234", parseChecker.captured);
    }

    private static class CapturingParseOk implements ConsumerE<String, NumberFormatException> {
        String captured;

        @Override
        public void accept(final String s) throws NumberFormatException {
            this.captured = s;
        }
    }

    private static class CapturingParseFail implements ConsumerE<String, NumberFormatException> {
        String captured;

        @Override
        public void accept(final String s) throws NumberFormatException {
            this.captured = s;
            throw new NumberFormatException("CapturingParseFail");
        }
    }
}
