let page = parseInt(document.body.dataset.page || "1");
let totalPages = parseInt(document.body.dataset.totalPages || "1");

function getFilters() {
    const params = new URLSearchParams();
    $("#filters").serializeArray().forEach(item => {
        if (item.value) params.append(item.name, item.value);
    });
    return params.toString();
}

$("#filters").on("submit", function(e) {
    e.preventDefault();
    page = 1;
    $.get("?", getFilters(), function(data) {
        $("#books-container").html("");
        for (let book of data.books) {
            $("#books-container").append(renderBook(book));
        }
        totalPages = data.total_pages;
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
    });
});

function renderBook(book) {
    return `
        <div class="col">
            <div class="card book-card h-100">
                <a href="${book.url}" target="_blank">
                    <img src="${book.coverUrl}" class="card-img-top" alt="${book.bookName}">
                </a>
                <div class="card-body p-2">
                    <h6 class="card-title small mb-1">
                        <a href="${book.url}" target="_blank" class="text-decoration-none text-dark">
                            ${book.bookName}
                        </a>
                    </h6>
                    <p class="card-text small mb-1">
                        <strong>★</strong> ${book.score} | <strong>Гл:</strong> ${book.chapters}
                    </p>
                    <p class="small text-muted">${book.cluster_label}</p>
                </div>
            </div>
        </div>`;
}

$(document).ready(function() {
    $('#cluster-select').select2({
        placeholder: "Выберите кластеры",
        allowClear: true,
        width: '100%'
    });
    $('#tag-select').select2({
        placeholder: "Выберите теги",
        allowClear: true,
        width: '100%'
    });
});
