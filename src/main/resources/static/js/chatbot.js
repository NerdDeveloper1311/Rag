document.addEventListener("DOMContentLoaded", () => {
    const queryInput = document.getElementById("knowledgeInput");
    const sendBtn = document.getElementById("sendBtn");

    // 텍스트 영역 자동 높이 조절
    queryInput.addEventListener("input", function() {
        this.style.height = "auto";
        this.style.height = (this.scrollHeight) + "px";
    });

    sendBtn.addEventListener("click", dispatchKnowledgeQuery);

    queryInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            dispatchKnowledgeQuery();
        }
    });
});

async function dispatchKnowledgeQuery() {
    const queryInput = document.getElementById("knowledgeInput");
    const chatBox = document.getElementById("knowledgeChatBox");
    const queryText = queryInput.value.trim();

    if (!queryText) return;

    // 1. 유저 말풍선 렌더링
    chatBox.innerHTML += `
        <div class="message-row user-row">
            <div class="chat-bubble user-bubble">${queryText}</div>
        </div>
    `;

    // 2. AI 대기 말풍선 생성
    const aiRowDiv = document.createElement("div");
    aiRowDiv.className = "message-row";
    const aiBubble = document.createElement("div");
    aiBubble.className = "chat-bubble ai-bubble";
    aiBubble.innerText = "지식 베이스 검색 및 답변 생성 중...";
    aiRowDiv.appendChild(aiBubble);
    chatBox.appendChild(aiRowDiv);
    chatBox.scrollTop = chatBox.scrollHeight;

    // 입력창 초기화
    queryInput.value = "";
    queryInput.style.height = "auto";

    try {
        // 백엔드 GetMapping("/chat?query=...") 스펙에 맞게 쿼리스트링 빌드
        const params = new URLSearchParams({ query: queryText });
        const response = await fetch(`/api/rag/chat?${params.toString()}`, {
            method: 'GET'
        });

        if (!response.ok) throw new Error("서ver와 통신 중 오류가 발생했습니다.");

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
            chunkStreamBuffer = lines.pop(); // 완결되지 않은 마지막 라인은 버퍼에 둠

            let stateChanged = false;
            for (let line of lines) {
                let cleanLine = line.trim();
                if (!cleanLine) continue;

                // SSE 'data:' 접두사 제거
                if (cleanLine.startsWith('data:')) {
                    cleanLine = cleanLine.replace(/^data:\s*/, '');
                }

                // 백엔드 보호 토큰 안전 복원 (\n 문법이 살아나야 marked가 정확히 파싱함)
                cleanLine = cleanLine.replaceAll('[SPACE]', ' ').replaceAll('[NEWLINE]', '\n');

                if (isFirst) {
                    aiBubble.innerText = '';
                    isFirst = false;
                }
                rawMarkdownBuffer += cleanLine;
                stateChanged = true;
            }

            // 청크 단위 마크다운 실시간 컴파일 반영
            if (stateChanged) {
                aiBubble.innerHTML = marked.parse(rawMarkdownBuffer);
                chatBox.scrollTop = chatBox.scrollHeight;
            }
        }

    } catch (err) {
        aiBubble.innerText = "⚠️ 답변을 가져오는 중 실패했습니다: " + err.message;
    }
}