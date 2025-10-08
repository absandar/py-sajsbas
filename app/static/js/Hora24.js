class Hora24 {
    constructor(selector) {
        const inputs = typeof selector === 'string'
            ? document.querySelectorAll(selector)
            : [selector];

        inputs.forEach(input => this._attachFormatter(input));
    }

    _attachFormatter(input) {
        input.addEventListener('input', () => {
            let raw = input.value.replace(/\D/g, '');
            if (raw.length > 4) raw = raw.slice(0, 4);

            if (raw.length >= 2) {
                let hh = raw.slice(0, 2);
                let mm = raw.slice(2, 4);
                input.value = hh + ':' + mm;
            } else {
                input.value = raw;
            }
        });

        input.addEventListener('keydown', (e) => {
            const val = input.value;
            const pos = input.selectionStart;

            if (e.key === 'Backspace' && val[pos - 1] === ':' && pos === 3) {
                e.preventDefault();
                input.value = val.slice(0, 1);
                input.setSelectionRange(1, 1);
            }
        });

        input.addEventListener('blur', () => {
            const val = input.value;
            if (!/^\d{2}:\d{2}$/.test(val)) {
                input.value = '';
                return;
            }

            const [hh, mm] = val.split(':');
            if (!this._isValid24Hour(hh, mm)) {
                input.value = '';
            }
        });
    }

    _isValid24Hour(hh, mm) {
        const h = parseInt(hh, 10);
        const m = parseInt(mm, 10);
        return (
            !isNaN(h) &&
            !isNaN(m) &&
            h >= 0 && h <= 23 &&
            m >= 0 && m <= 59
        );
    }
}