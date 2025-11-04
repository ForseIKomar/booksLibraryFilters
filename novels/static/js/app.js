(() => {
    console.log("✅ app.js loaded");

    // --- Настройка начальных параметров страницы ---
    let page = parseInt(document.body.dataset.page ?? "1");
    let totalPages = parseInt(document.body.dataset.totalPages ?? "1");
    if (isNaN(page)) page = 1;
    if (isNaN(totalPages)) totalPages = 1;

    /* ==========================================================
       🔹 Универсальные функции
       ========================================================== */

    // Получение фильтров
    function getFilters() {
        const params = new URLSearchParams();
        $("#filters").serializeArray().forEach(item => {
            if (item.value) params.append(item.name, item.value);
        });
        return params.toString();
    }

    // Безопасное получение CSRF-токена (для Django)
    function getCsrfToken() {
        const name = "csrftoken=";
        const cookies = document.cookie.split(";");
        for (let c of cookies) {
            c = c.trim();
            if (c.startsWith(name)) return c.substring(name.length, c.length);
        }
        return "";
    }

    // Рендер карточки книги
    function renderBook(book) {
        const lastStats = (book.last_view || book.last_click)
            ? `<p class="small text-muted mt-1 mb-0" style="font-size: 0.75rem;">
                 ${book.last_view ? `👁️ Просмотр: ${book.last_view}` : ""}
                 ${book.last_click ? `<br>🖱️ Клик: ${book.last_click}` : ""}
               </p>`
            : "";

        return `
            <div class="col">
                <div class="card book-card h-100" data-book-id="${book.bookId}">
                    <a href="${book.url}" target="_blank" class="book-link">
                        <img src="${book.cover_url}" class="card-img-top" referrerpolicy="no-referrer" alt="${book.bookName}">
                    </a>
                    <div class="card-body p-2">
                        <h6 class="card-title small mb-1">
                            <a href="${book.url}" target="_blank" class="text-decoration-none text-dark book-link">
                                ${book.bookName}
                            </a>
                        </h6>
                        <p class="card-text small mb-1">${book.site}</p>
                        <p class="card-text small mb-1">
                            <strong>★</strong> ${book.score || "Не оценено"} |
                            <strong>Гл:</strong> ${book.free || 0} / ${book.chapters || "?"}
                        </p>
                        <p class="small text-muted text-truncate-3" title="${book.description || ""}">
                            ${book.description || ""}
                        </p>
                        ${lastStats}
                    </div>
                </div>
            </div>`;
    }

    /* ==========================================================
       🔹 AJAX-запросы на действия пользователя
       ========================================================== */

    // Отправка факта просмотра (один раз на каждую книгу)
    function markBooksAsViewed(books) {
        if (!books || !books.length) return;
        const ids = books.map(b => b.bookId);
        fetch("/api/book/view_batch/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-CSRFToken": getCsrfToken(),
            },
            body: JSON.stringify({ book_ids: ids })
        }).then(r => {
            if (!r.ok) console.warn("view_batch failed", r.status);
        }).catch(err => console.warn("view error", err));
    }

    // Отправка факта клика
    $(document).on("click", ".book-link", function () {
        const card = $(this).closest(".book-card");
        const bookId = card.data("book-id");
        if (bookId) {
            fetch(`/api/book/click/${bookId}/`, {
                method: "POST",
                headers: { "X-CSRFToken": getCsrfToken() },
            }).catch(err => console.warn("click error", err));
        }
    });

    /* ==========================================================
       🔹 Обработка фильтров и пагинации
       ========================================================== */

    $("#filters").on("submit", function(e) {
        e.preventDefault();
        page = 1;
        $.get("?", getFilters(), function(data) {
            $("#books-container").html("");
            for (let book of data.books) {
                $("#books-container").append(renderBook(book));
            }
            totalPages = data.total_pages;
            markBooksAsViewed(data.books);
        });
    });

    $("#load-more").on("click", function() {
        if (page >= totalPages) return;
        page++;
        $.get("?", getFilters() + "&page=" + page, function(data) {
            for (let book of data.books) {
                $("#books-container").append(renderBook(book));
            }
            totalPages = data.total_pages;
            markBooksAsViewed(data.books);
        });
    });

    /* ==========================================================
       🔹 Инициализация select2 и регистрация стартовых просмотров
       ========================================================== */

    $(document).ready(function() {
        // Инициализация Select2 для всех селектов, если они есть
        const selectIds = [
            "#genre-select", "#fandom-select", "#tag-select", "#site-select",
            "#exclude_genre", "#exclude_tag", "#exclude_fandom"
        ];
        selectIds.forEach(id => {
            if ($(id).length) {
                $(id).select2({
                    placeholder: "Выберите",
                    allowClear: true,
                    width: "100%"
                });
            }
        });

        // Отправляем просмотры по уже отрисованным книгам
        const initialBooks = [];
        $(".book-card").each(function() {
            const id = $(this).data("book-id");
            if (id) initialBooks.push({ bookId: id });
        });
        markBooksAsViewed(initialBooks);
    });
})();
