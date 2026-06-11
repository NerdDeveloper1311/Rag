let selectedFilesArray = [];

document.addEventListener("DOMContentLoaded", () => {
    const fileInput = document.getElementById("multiImageFiles");
    const textInput = document.getElementById("compositeInput");
    const submitBtn = document.getElementById("submitBtn");

    textInput.addEventListener("input", function() {
        this.style.height = "auto";
        this.style.height = (this.scrollHeight) + "px";
    });

    fileInput.addEventListener("change", handleFileSelection);
    submitBtn.addEventListener("click", dispatchMultimodalRequest);

    textInput.addEventListener("keydown", (e) => {
        if(e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            dispatchMultimodalRequest();
        }
    });
});

function handleFileSelection(event) {
    const previewContainer = document.getElementById("previewThumbnails");
    const files = event.target.files;
    for (let file of files) {
        selectedFilesArray.push(file);
        const reader = new FileReader();
        reader.onload = function(e) {
            const img = document.createElement("img");
            img.src = e.target.result;
            img.className = "thumb-img-frame";
            previewContainer.appendChild(img);
        };
        reader.readAsDataURL(file);
    }
    event.target.value = '';
}

async function dispatchMultimodalRequest() {
    const queryInput = document.getElementById("compositeInput");
    const chatBox = document.getElementById("multimodalChatBox");
    const queryText = queryInput.value.trim();

    if (queryText === "" && selectedFilesArray.length === 0) return;

    // 유저 말풍선 내부에 첨부된 이미지들을 시각적으로 확인할 수 있도록 HTML 구성
    let imageHtml = "";
    if (selectedFilesArray.length > 0) {
        imageHtml = `<div class="user-attached-images" style="display: flex; gap: 6px; margin-bottom: 8px; flex-wrap: wrap; justify-content: flex-end;">`;
        selectedFilesArray.forEach(file => {
            const imgUrl = URL.createObjectURL(file);
            imageHtml += `<img src="${imgUrl}" style="width: 75px; height: 75px; object-fit: cover; border-radius: 8px; border: 1px solid #555;" />`;
        });
        imageHtml += `</div>`;
    }

    let userPromptMsg = queryText;
    if (userPromptMsg === "" && selectedFilesArray.length > 0) {
        userPromptMsg = `<span style="color: #aaa; font-style: italic;">📸 이미지 분석 요청</span>`;
    }

    // 채팅창에 유저 메시지(이미지 포함) 추가
    chatBox.innerHTML += `
        <div class="message-row user-row">
            <div class="chat-bubble user-bubble">
                ${imageHtml}
                <div>${userPromptMsg}</div>
            </div>
        </div>
    `;

    // 모델이 연산 및 생각 중인 현황을 부트스트랩 스피너와 함께 동적으로 연출
    const aiRowDiv = document.createElement("div");
    aiRowDiv.className = "message-row";
    const aiBubble = document.createElement("div");
    aiBubble.className = "chat-bubble ai-bubble";
    aiBubble.innerHTML = `
        <div class="thinking-status" style="display: flex; align-items: center; gap: 10px; color: #b4b4b4;">
            <div class="spinner-border spinner-border-sm text-light" role="status" style="width: 1rem; height: 1rem;"></div>
            <span>이미지 전송 완료 및 지식 엔진 분석 중...</span>
        </div>
    `;
    aiRowDiv.appendChild(aiBubble);
    chatBox.appendChild(aiRowDiv);
    chatBox.scrollTop = chatBox.scrollHeight;

    // UI 입력 및 썸네일 영역 즉시 초기화
    queryInput.value = "";
    queryInput.style.height = "auto";
    document.getElementById("previewThumbnails").innerHTML = "";
    document.getElementById("multiImageFiles").value = "";

    // 폼 데이터를 빌드한 직후 백업 배열을 비워 연속 요청 시 중복 버그 원천 차단
    const formData = new FormData();
    selectedFilesArray.forEach(file => formData.append("files", file));
    formData.append("query", queryText);
    selectedFilesArray = [];

    try {
        const response = await fetch('/api/rag/chat/multimodal', {
            method: 'POST',
            body: formData
        });
        if (!response.ok) throw new Error("서버와의 통신에 실패했습니다.");

        const reader = response.body.getReader();
        const decoder = new TextDecoder("utf-8");
        let isFirst = true;
        let rawMarkdownBuffer = "";
        let chunkStreamBuffer = "";

        while (true) {
            const { value, done } = await reader.read();
            if (done) break;

            chunkStreamBuffer += decoder.decode(value, { stream: true });
            const lines = chunkStreamBuffer.split('\n');
            chunkStreamBuffer = lines.pop();
            let stateChanged = false;

            for (let line of lines) {
                let cleanLine = line.trim();
                if (!cleanLine) continue;
                if (cleanLine.startsWith('data:')) {
                    cleanLine = cleanLine.replace(/^data:\s*/, '');
                }

                cleanLine = cleanLine.replaceAll('[SPACE]', ' ').replaceAll('[NEWLINE]', '\n');

                if (isFirst) {
                    aiBubble.innerHTML = ''; // 첫 토큰 수신 시 생각 중 로딩바 제거
                    isFirst = false;
                }
                rawMarkdownBuffer += cleanLine;
                stateChanged = true;
            }

            // 💡 [2번 기능 반영] 청크가 쌓일 때마다 화살표 기호들을 실시간 치환하여 마크다운 렌더링에 전달
            if (stateChanged) {
                let processedMarkdown = rawMarkdownBuffer
                    .replaceAll('->', '→')
                    .replaceAll('=>', '⇒')
                    .replaceAll('$\\rightarrow$', '→')
                    .replaceAll('\\rightarrow', '→');

                aiBubble.innerHTML = marked.parse(processedMarkdown);
                chatBox.scrollTop = chatBox.scrollHeight;
            }
        }

        if (rawMarkdownBuffer.length > 0) {
            const fallbackText = queryText || "📸 [발 이미지 전송됨] 분석 완료";
            appendFeedbackInterface(aiRowDiv, fallbackText, aiBubble.innerText);
        }

    } catch (err) {
        aiBubble.innerText = "오류 발생: " + err.message;
    }
}

function appendFeedbackInterface(targetContainer, q, a) {
    const feedbackDiv = document.createElement('div');
    feedbackDiv.className = 'feedback-buttons';
    feedbackDiv.innerHTML = `
        <button class="feedback-btn" onclick="triggerFeedbackEmit(this, 'like')"><i class="bi bi-hand-thumbs-up"></i></button>
        <button class="feedback-btn" onclick="triggerFeedbackEmit(this, 'dislike')"><i class="bi bi-hand-thumbs-down"></i></button>
    `;
    feedbackDiv.dataset.question = q;
    feedbackDiv.dataset.answer = a;
    targetContainer.appendChild(feedbackDiv);
}

async function triggerFeedbackEmit(btnElement, evalType) {
    const wrapper = btnElement.parentElement;
    const q = wrapper.dataset.question;
    const a = wrapper.dataset.answer;

    try {
        const response = await fetch('/api/rag/feedback', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ question: q, answer: a, score: evalType === 'like' ? 1 : -1 })
        });
        if (response.ok) {
            btnElement.classList.add(evalType === 'like' ? 'active-like' : 'active-dislike');
        }
    } catch (e) {
        console.error(e);
    }
}